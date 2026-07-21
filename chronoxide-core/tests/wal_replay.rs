use std::io::Write;
use std::time::Duration;

use chrono::TimeDelta;
use chronoxide_core::event_time::EventTimePolicy;
use chronoxide_core::labels::{
    DefaultSymbolTable, FlatInternedLabelSetStore, LabelSetStore, METRIC_NAME_LABEL,
};
use chronoxide_core::promql::{normalize_label_name, normalize_metric_name};
use chronoxide_core::storage::head::{
    CounterResetHint, FloatEncoding, HeadBuffer, HeadConfig, IntEncoding, SeriesSamples,
};
use chronoxide_core::storage::segment::{LabelMatcher, SegmentSelector};
use chronoxide_core::storage::wal::{
    OtlpWalBatch, TransportOffset, WalRecordType, WalWriter, encode_otlp_batch_payload,
    write_wal_record,
};
use chronoxide_core::storage::wal_replay::{
    WalReplayStopReason, replay_wal_file_into_head, replay_wal_file_into_head_from_checkpoint,
};
use opentelemetry_proto::tonic;
use prost::Message;

#[test]
fn wal_replay_rebuilds_queryable_head_from_otlp_batches() {
    let tempdir = tempfile::tempdir().unwrap();
    let wal_path = tempdir.path().join("wal-000000.log");
    let mut writer = WalWriter::create(&wal_path).unwrap();
    writer
        .append_otlp_batch(&OtlpWalBatch {
            payload: request(
                vec![kv("service.name", "checkout")],
                vec![
                    number_metric("cpu.usage", "backend-1", 5_000, NumberValue::Float(1.5)),
                    number_metric("request.count", "backend-1", 6_000, NumberValue::Int(42)),
                ],
            )
            .encode_to_vec(),
            ..wal_metadata(6_000)
        })
        .unwrap();
    writer.flush().unwrap();
    drop(writer);

    let mut head = test_head();
    let mut labels = FlatInternedLabelSetStore::<DefaultSymbolTable>::default();
    let outcome = replay_wal_file_into_head(
        &wal_path,
        EventTimePolicy::default(),
        &mut head,
        &mut labels,
    )
    .unwrap();
    assert!(outcome.completed_windows.is_empty());
    let partition = outcome.partition.as_ref().unwrap();
    assert_eq!(partition.topic, "metrics");
    assert_eq!(partition.partition, 0);
    let report = outcome.report;

    assert_eq!(report.batches_replayed, 1);
    assert_eq!(report.datapoints_replayed, 2);
    assert_eq!(report.stop_reason, None);

    let cpu = head
        .query_selector(
            &labels,
            &SegmentSelector::with_metric(
                "cpu.usage",
                vec![LabelMatcher::eq("pod.name", "backend-1")],
            ),
            0,
            10_000,
        )
        .unwrap();
    assert_eq!(cpu.len(), 1);
    assert_eq!(cpu[0].samples, vec![(5_000, 1.5)]);
    assert!(cpu[0].labels.iter().any(|(key, value)| {
        key == METRIC_NAME_LABEL && value == normalize_metric_name("cpu.usage")
    }));

    let count = head
        .query_selector(
            &labels,
            &SegmentSelector::with_metric("request.count", vec![]),
            0,
            10_000,
        )
        .unwrap();
    assert_eq!(count.len(), 1);
    assert_eq!(count[0].samples, vec![(6_000, 42.0)]);
}

#[test]
fn wal_replay_rejects_nonempty_recovery_state() {
    let tempdir = tempfile::tempdir().unwrap();
    let wal_path = tempdir.path().join("wal-replay-twice.log");
    let mut writer = WalWriter::create(&wal_path).unwrap();
    writer
        .append_otlp_batch(&OtlpWalBatch {
            payload: request(
                vec![],
                vec![number_metric(
                    "fresh.state.gauge",
                    "one",
                    5_000,
                    NumberValue::Float(1.0),
                )],
            )
            .encode_to_vec(),
            ..wal_metadata(5_000)
        })
        .unwrap();
    writer.flush().unwrap();
    drop(writer);

    let mut head = test_head();
    let mut labels = FlatInternedLabelSetStore::<DefaultSymbolTable>::default();
    let first = replay_wal_file_into_head(
        &wal_path,
        EventTimePolicy::default(),
        &mut head,
        &mut labels,
    )
    .unwrap();
    assert!(first.completed_windows.is_empty());

    let error = replay_wal_file_into_head(
        &wal_path,
        EventTimePolicy::default(),
        &mut head,
        &mut labels,
    )
    .unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
}

#[test]
fn wal_replay_returns_every_completed_head_window() {
    let tempdir = tempfile::tempdir().unwrap();
    let wal_path = tempdir.path().join("wal-multiple-head-windows.log");
    let mut writer = WalWriter::create(&wal_path).unwrap();
    writer
        .append_otlp_batch(&OtlpWalBatch {
            payload: request(
                vec![],
                vec![gauge_metric(
                    "windowed.gauge",
                    vec![
                        number_datapoint("same", 5_000, NumberValue::Float(1.0)),
                        number_datapoint("same", 15_000, NumberValue::Float(2.0)),
                        number_datapoint("same", 25_000, NumberValue::Float(3.0)),
                    ],
                )],
            )
            .encode_to_vec(),
            ..wal_metadata(25_000)
        })
        .unwrap();
    writer.flush().unwrap();
    drop(writer);

    let mut head = test_head();
    let mut labels = FlatInternedLabelSetStore::<DefaultSymbolTable>::default();
    let outcome = replay_wal_file_into_head(
        &wal_path,
        EventTimePolicy::default(),
        &mut head,
        &mut labels,
    )
    .unwrap();

    assert_eq!(outcome.report.datapoints_replayed, 3);
    assert_eq!(outcome.completed_windows.len(), 2);
    assert_eq!(
        outcome
            .completed_windows
            .iter()
            .map(|window| (window.start_ms, window.end_ms))
            .collect::<Vec<_>>(),
        vec![(0, 10_000), (10_000, 20_000)]
    );

    let mut windows = outcome.completed_windows;
    windows.extend(head.drain_windows());
    assert_eq!(windows.len(), 3);
    let mut samples = windows
        .into_iter()
        .flat_map(|window| window.into_series_samples().unwrap())
        .flat_map(|(_, samples)| match samples {
            SeriesSamples::Float { samples, .. } => samples,
            other => panic!("expected float samples, got {other:?}"),
        })
        .collect::<Vec<_>>();
    samples.sort_by_key(|sample| sample.0);
    assert_eq!(samples, vec![(5_000, 1.0), (15_000, 2.0), (25_000, 3.0)]);
}

#[test]
fn wal_replay_rejects_batches_from_more_than_one_partition() {
    let tempdir = tempfile::tempdir().unwrap();
    let wal_path = tempdir.path().join("wal-mixed-partitions.log");
    let mut writer = WalWriter::create(&wal_path).unwrap();
    for (partition, timestamp_ms) in [(0_i32, 5_000_u64), (1, 6_000)] {
        writer
            .append_otlp_batch(&OtlpWalBatch {
                partition,
                offset: i64::from(partition),
                payload: request(
                    vec![],
                    vec![number_metric(
                        "partition.gauge",
                        "one",
                        timestamp_ms,
                        NumberValue::Float(1.0),
                    )],
                )
                .encode_to_vec(),
                ..wal_metadata(i64::try_from(timestamp_ms).unwrap())
            })
            .unwrap();
    }
    writer.flush().unwrap();
    drop(writer);

    let mut head = test_head();
    let mut labels = FlatInternedLabelSetStore::<DefaultSymbolTable>::default();
    let error = replay_wal_file_into_head(
        &wal_path,
        EventTimePolicy::default(),
        &mut head,
        &mut labels,
    )
    .unwrap_err();

    assert_eq!(error.kind(), std::io::ErrorKind::Unsupported);

    // Fatal replay errors are not transactional. Recovery callers must use
    // fresh state and discard it on error rather than querying this prefix.
    assert_eq!(labels.len(), 1);
    let partial = head
        .query_selector(
            &labels,
            &SegmentSelector::with_metric("partition.gauge", vec![]),
            0,
            10_000,
        )
        .unwrap();
    assert_eq!(partial.len(), 1);
    assert_eq!(partial[0].samples, vec![(5_000, 1.0)]);
}

#[test]
fn wal_replay_retains_out_of_order_windows_in_the_supplied_head() {
    let tempdir = tempfile::tempdir().unwrap();
    let wal_path = tempdir.path().join("wal-out-of-order.log");
    let mut writer = WalWriter::create(&wal_path).unwrap();
    writer
        .append_otlp_batch(&OtlpWalBatch {
            payload: request(
                vec![],
                vec![gauge_metric(
                    "out.of.order.gauge",
                    vec![
                        number_datapoint("same", 15_000, NumberValue::Float(1.0)),
                        number_datapoint("same", 9_500, NumberValue::Float(2.0)),
                    ],
                )],
            )
            .encode_to_vec(),
            ..wal_metadata(15_000)
        })
        .unwrap();
    writer.flush().unwrap();
    drop(writer);

    let config = HeadConfig::with_block_size(
        Duration::from_secs(10),
        2,
        FloatEncoding::Gorilla,
        IntEncoding::DeltaZigZag,
    )
    .with_out_of_order_time_window(Duration::from_secs(6));
    let mut head = HeadBuffer::new(config).unwrap();
    let mut labels = FlatInternedLabelSetStore::<DefaultSymbolTable>::default();
    let outcome = replay_wal_file_into_head(
        &wal_path,
        EventTimePolicy::default(),
        &mut head,
        &mut labels,
    )
    .unwrap();

    assert!(outcome.completed_windows.is_empty());
    assert_eq!(outcome.report.datapoints_replayed, 2);
    let mut windows = head.drain_windows();
    windows.sort_by_key(|window| window.start_ms);
    assert_eq!(windows.len(), 2);
    assert_eq!(
        windows
            .iter()
            .map(|window| (window.start_ms, window.end_ms))
            .collect::<Vec<_>>(),
        vec![(0, 10_000), (10_000, 20_000)]
    );
}

#[test]
fn wal_replay_validates_checkpoint_meta_and_replays_the_full_wal() {
    let tempdir = tempfile::tempdir().unwrap();
    let wal_path = tempdir.path().join("wal-000001.log");
    let mut writer = WalWriter::create(&wal_path).unwrap();
    writer
        .append_otlp_batch(&OtlpWalBatch {
            payload: request(
                vec![],
                vec![number_metric(
                    "cpu.usage",
                    "before",
                    5_000,
                    NumberValue::Float(1.0),
                )],
            )
            .encode_to_vec(),
            ..wal_metadata(5_000)
        })
        .unwrap();
    let checkpoint = writer
        .append_checkpoint_and_publish(
            tempdir.path(),
            1_725_000_000_000,
            vec![TransportOffset {
                topic: "metrics".to_string(),
                partition: 0,
                next_offset: 10,
            }],
        )
        .unwrap();
    writer
        .append_otlp_batch(&OtlpWalBatch {
            payload: request(
                vec![],
                vec![number_metric(
                    "cpu.usage",
                    "after",
                    6_000,
                    NumberValue::Float(2.0),
                )],
            )
            .encode_to_vec(),
            ..wal_metadata(6_000)
        })
        .unwrap();
    writer.flush().unwrap();
    drop(writer);

    let mut head = test_head();
    let mut labels = FlatInternedLabelSetStore::<DefaultSymbolTable>::default();
    let outcome = replay_wal_file_into_head_from_checkpoint(
        &wal_path,
        tempdir.path(),
        EventTimePolicy::default(),
        &mut head,
        &mut labels,
    )
    .unwrap();
    assert!(outcome.completed_windows.is_empty());
    let partition = outcome.partition.as_ref().unwrap();
    assert_eq!(partition.topic, "metrics");
    assert_eq!(partition.partition, 0);
    let report = outcome.report;

    assert_eq!(report.checkpoint_lsn, Some(checkpoint.wal_lsn));
    assert_eq!(report.replay_start_lsn, 0);
    assert_eq!(report.batches_replayed, 2);
    assert_eq!(report.datapoints_replayed, 2);

    let mut results = head
        .query_selector(
            &labels,
            &SegmentSelector::with_metric("cpu.usage", vec![]),
            0,
            10_000,
        )
        .unwrap();
    results.sort_by_key(|result| result.samples[0].0);
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].samples, vec![(5_000, 1.0)]);
    assert_eq!(results[1].samples, vec![(6_000, 2.0)]);
    assert!(results.iter().any(|result| {
        result
            .labels
            .iter()
            .any(|(key, value)| key == normalize_label_name("pod.name") && value == "before")
    }));
    assert!(results.iter().any(|result| {
        result
            .labels
            .iter()
            .any(|(key, value)| key == normalize_label_name("pod.name") && value == "after")
    }));
}

#[test]
fn checkpoint_replay_matches_full_replay_including_pre_checkpoint_samples() {
    let tempdir = tempfile::tempdir().unwrap();
    let wal_path = tempdir.path().join("wal-checkpoint-reset-state.log");
    let mut writer = WalWriter::create(&wal_path).unwrap();

    writer
        .append_otlp_batch(&OtlpWalBatch {
            payload: request(
                vec![],
                vec![
                    cumulative_histogram_metric(
                        "checkpoint.pre_only",
                        10_000,
                        1_000,
                        1,
                        1.0,
                        vec![1, 0],
                    ),
                    cumulative_histogram_metric(
                        "checkpoint.histogram",
                        10_000,
                        1_000,
                        10,
                        20.0,
                        vec![4, 6],
                    ),
                    cumulative_exponential_histogram_metric(
                        "checkpoint.exponential_histogram",
                        10_000,
                        1_000,
                        8,
                        16.0,
                        2,
                        vec![3, 3],
                    ),
                ],
            )
            .encode_to_vec(),
            ..wal_metadata(10_000)
        })
        .unwrap();
    let checkpoint = writer
        .append_checkpoint_and_publish(
            tempdir.path(),
            10_500,
            vec![TransportOffset {
                topic: "metrics".to_string(),
                partition: 0,
                next_offset: 2,
            }],
        )
        .unwrap();

    writer
        .append_otlp_batch(&OtlpWalBatch {
            payload: request(
                vec![],
                vec![
                    cumulative_histogram_metric(
                        "checkpoint.histogram",
                        20_000,
                        1_000,
                        15,
                        30.0,
                        vec![6, 9],
                    ),
                    cumulative_exponential_histogram_metric(
                        "checkpoint.exponential_histogram",
                        20_000,
                        1_000,
                        13,
                        26.0,
                        3,
                        vec![5, 5],
                    ),
                ],
            )
            .encode_to_vec(),
            ..wal_metadata(20_000)
        })
        .unwrap();
    writer
        .append_otlp_batch(&OtlpWalBatch {
            payload: request(
                vec![],
                vec![
                    cumulative_histogram_metric(
                        "checkpoint.histogram",
                        30_000,
                        25_000,
                        2,
                        4.0,
                        vec![1, 1],
                    ),
                    cumulative_exponential_histogram_metric(
                        "checkpoint.exponential_histogram",
                        30_000,
                        25_000,
                        1,
                        2.0,
                        0,
                        vec![1],
                    ),
                ],
            )
            .encode_to_vec(),
            ..wal_metadata(30_000)
        })
        .unwrap();
    writer.flush().unwrap();
    drop(writer);

    let mut full_head = test_head_with_window(Duration::from_secs(3_600));
    let mut full_labels = FlatInternedLabelSetStore::<DefaultSymbolTable>::default();
    let full_outcome = replay_wal_file_into_head(
        &wal_path,
        EventTimePolicy::default(),
        &mut full_head,
        &mut full_labels,
    )
    .unwrap();
    assert!(full_outcome.completed_windows.is_empty());
    let full_samples = full_head
        .drain()
        .unwrap()
        .into_series_samples()
        .unwrap()
        .into_iter()
        .collect::<std::collections::BTreeMap<_, _>>();

    let mut checkpoint_head = test_head_with_window(Duration::from_secs(3_600));
    let mut checkpoint_labels = FlatInternedLabelSetStore::<DefaultSymbolTable>::default();
    let report = replay_wal_file_into_head_from_checkpoint(
        &wal_path,
        tempdir.path(),
        EventTimePolicy::default(),
        &mut checkpoint_head,
        &mut checkpoint_labels,
    )
    .unwrap()
    .report;
    let checkpoint_samples = checkpoint_head
        .drain()
        .unwrap()
        .into_series_samples()
        .unwrap()
        .into_iter()
        .collect::<std::collections::BTreeMap<_, _>>();

    assert_eq!(report.checkpoint_lsn, Some(checkpoint.wal_lsn));
    assert_eq!(report.batches_replayed, 3);
    assert_eq!(report.datapoints_replayed, 7);
    assert_eq!(checkpoint_labels.len(), 3);
    assert_eq!(checkpoint_samples.len(), 3);
    assert_eq!(checkpoint_samples, full_samples);

    let SeriesSamples::Histogram {
        samples: full_histogram,
    } = full_samples.get(&1.into()).unwrap()
    else {
        panic!("expected full histogram samples");
    };
    let SeriesSamples::Histogram {
        samples: checkpoint_histogram,
    } = checkpoint_samples.get(&1.into()).unwrap()
    else {
        panic!("expected checkpoint histogram samples");
    };
    assert_eq!(checkpoint_histogram, full_histogram);
    assert_eq!(
        checkpoint_histogram
            .iter()
            .map(|(timestamp_ms, value)| (timestamp_ms, value.metadata.reset_hint))
            .collect::<Vec<_>>(),
        vec![
            (&10_000, CounterResetHint::Unknown),
            (&20_000, CounterResetHint::NotCounterReset),
            (&30_000, CounterResetHint::CounterReset),
        ]
    );

    let SeriesSamples::ExponentialHistogram {
        samples: full_exponential_histogram,
    } = full_samples.get(&2.into()).unwrap()
    else {
        panic!("expected full exponential histogram samples");
    };
    let SeriesSamples::ExponentialHistogram {
        samples: checkpoint_exponential_histogram,
    } = checkpoint_samples.get(&2.into()).unwrap()
    else {
        panic!("expected checkpoint exponential histogram samples");
    };
    assert_eq!(checkpoint_exponential_histogram, full_exponential_histogram);
    assert_eq!(
        checkpoint_exponential_histogram
            .iter()
            .map(|(timestamp_ms, value)| (timestamp_ms, value.metadata.reset_hint))
            .collect::<Vec<_>>(),
        vec![
            (&10_000, CounterResetHint::Unknown),
            (&20_000, CounterResetHint::NotCounterReset),
            (&30_000, CounterResetHint::CounterReset),
        ]
    );
}

#[test]
fn wal_replay_stops_at_first_invalid_record_and_keeps_prior_samples() {
    let tempdir = tempfile::tempdir().unwrap();
    let wal_path = tempdir.path().join("wal-000002.log");
    let mut writer = WalWriter::create(&wal_path).unwrap();
    writer
        .append_otlp_batch(&OtlpWalBatch {
            payload: request(
                vec![],
                vec![number_metric(
                    "cpu.usage",
                    "backend-1",
                    5_000,
                    NumberValue::Float(1.5),
                )],
            )
            .encode_to_vec(),
            ..wal_metadata(5_000)
        })
        .unwrap();
    let invalid_lsn = writer.current_offset().unwrap();
    writer.flush().unwrap();
    drop(writer);

    let mut file = std::fs::OpenOptions::new()
        .append(true)
        .open(&wal_path)
        .unwrap();
    file.write_all(b"torn").unwrap();
    drop(file);

    let mut head = test_head();
    let mut labels = FlatInternedLabelSetStore::<DefaultSymbolTable>::default();
    let report = replay_wal_file_into_head(
        &wal_path,
        EventTimePolicy::default(),
        &mut head,
        &mut labels,
    )
    .unwrap()
    .report;

    assert_eq!(report.batches_replayed, 1);
    assert_eq!(report.stopped_at_lsn, Some(invalid_lsn));
    assert_eq!(report.stop_reason, Some(WalReplayStopReason::UnexpectedEof));

    let results = head
        .query_selector(
            &labels,
            &SegmentSelector::with_metric("cpu.usage", vec![]),
            0,
            10_000,
        )
        .unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].samples, vec![(5_000, 1.5)]);
}

#[test]
fn wal_replay_rejects_malformed_otlp_and_continues_with_later_valid_batches() {
    let tempdir = tempfile::tempdir().unwrap();
    let wal_path = tempdir.path().join("wal-malformed-otlp.log");
    let mut writer = WalWriter::create(&wal_path).unwrap();
    writer
        .append_otlp_batch(&OtlpWalBatch {
            payload: request(
                vec![],
                vec![number_metric(
                    "cpu.usage",
                    "before",
                    5_000,
                    NumberValue::Float(1.0),
                )],
            )
            .encode_to_vec(),
            ..wal_metadata(5_000)
        })
        .unwrap();
    writer
        .append_otlp_batch(&OtlpWalBatch {
            // Truncated length-delimited protobuf field. The OBAT framing and outer WAL CRC
            // remain valid, so this is a rejected source batch rather than WAL corruption.
            payload: vec![0x0a, 0x02, 0x00],
            ..wal_metadata(5_500)
        })
        .unwrap();
    writer
        .append_otlp_batch(&OtlpWalBatch {
            payload: request(
                vec![],
                vec![number_metric(
                    "cpu.usage",
                    "after",
                    6_000,
                    NumberValue::Float(2.0),
                )],
            )
            .encode_to_vec(),
            ..wal_metadata(6_000)
        })
        .unwrap();
    writer.flush().unwrap();
    drop(writer);

    let mut head = test_head();
    let mut labels = FlatInternedLabelSetStore::<DefaultSymbolTable>::default();
    let report = replay_wal_file_into_head(
        &wal_path,
        EventTimePolicy::default(),
        &mut head,
        &mut labels,
    )
    .unwrap()
    .report;

    assert_eq!(report.records_read, 3);
    assert_eq!(report.batches_replayed, 2);
    assert_eq!(report.invalid_otlp_batches, 1);
    assert_eq!(report.datapoints_replayed, 2);
    assert_eq!(report.stopped_at_lsn, None);
    assert_eq!(report.stop_reason, None);

    let results = head
        .query_selector(
            &labels,
            &SegmentSelector::with_metric("cpu.usage", vec![]),
            0,
            10_000,
        )
        .unwrap();
    assert_eq!(results.len(), 2);
    let samples = results
        .into_iter()
        .map(|result| result.samples)
        .collect::<Vec<_>>();
    assert!(
        samples
            .iter()
            .any(|samples| samples.as_slice() == [(5_000, 1.0)])
    );
    assert!(
        samples
            .iter()
            .any(|samples| samples.as_slice() == [(6_000, 2.0)])
    );
}

#[test]
fn wal_replay_rejects_v1_batch_payload_as_an_unsupported_recovery_format() {
    let tempdir = tempfile::tempdir().unwrap();
    let wal_path = tempdir.path().join("wal-v1-unsupported.log");
    let mut payload = encode_otlp_batch_payload(&OtlpWalBatch {
        payload: request(vec![], vec![]).encode_to_vec(),
        ..wal_metadata(5_000)
    })
    .unwrap();
    payload[4..6].copy_from_slice(&1u16.to_le_bytes());
    let mut file = std::fs::File::create(&wal_path).unwrap();
    write_wal_record(&mut file, WalRecordType::OtlpBatch, &payload).unwrap();
    file.flush().unwrap();
    drop(file);

    let mut head = test_head();
    let mut labels = FlatInternedLabelSetStore::<DefaultSymbolTable>::default();
    let error = replay_wal_file_into_head(
        &wal_path,
        EventTimePolicy::default(),
        &mut head,
        &mut labels,
    )
    .unwrap_err();

    assert_eq!(error.kind(), std::io::ErrorKind::Unsupported);
    assert_eq!(labels.len(), 0);
    assert!(head.window_range().is_none());
}

#[test]
fn wal_replay_applies_captured_time_policy_before_interning_for_every_otlp_kind() {
    let tempdir = tempfile::tempdir().unwrap();
    let wal_path = tempdir.path().join("wal-event-time-policy.log");
    let mut writer = WalWriter::create(&wal_path).unwrap();
    writer
        .append_otlp_batch(&OtlpWalBatch {
            source_timestamp_ms: 0,
            captured_at_ms: 100_000,
            payload: request(
                vec![],
                vec![
                    policy_gauge_metric(),
                    policy_sum_metric(),
                    policy_histogram_metric(),
                    policy_exponential_histogram_metric(),
                    policy_summary_metric(),
                ],
            )
            .encode_to_vec(),
            ..wal_metadata(100_000)
        })
        .unwrap();
    writer.flush().unwrap();
    drop(writer);

    let policy = EventTimePolicy::new(TimeDelta::seconds(10), TimeDelta::seconds(5), true);
    let mut head = test_head();
    let mut labels = FlatInternedLabelSetStore::<DefaultSymbolTable>::default();

    let report = replay_wal_file_into_head(&wal_path, policy, &mut head, &mut labels)
        .unwrap()
        .report;

    assert_eq!(report.batches_replayed, 1);
    assert_eq!(report.policy_accepted_datapoints, 5);
    assert_eq!(report.dropped_too_old_datapoints, 5);
    assert_eq!(report.dropped_too_future_datapoints, 5);
    assert_eq!(report.missing_timestamp_datapoints, 5);
    assert_eq!(report.datapoints_replayed, 5);
    assert_eq!(
        labels.len(),
        5,
        "rejected datapoints must not intern labels"
    );
    for rejected_value in ["old", "future", "missing"] {
        assert_eq!(
            labels.symbols().lookup(rejected_value),
            None,
            "rejected datapoints must not intern symbol {rejected_value}"
        );
    }

    let decoded = head
        .drain()
        .expect("accepted samples create a head window")
        .into_series_samples()
        .unwrap();
    assert_eq!(decoded.len(), 5);
    for (_, samples) in decoded {
        let (timestamp_ms, sample_count) = match samples {
            SeriesSamples::Float { samples, .. } => (samples[0].0, samples.len()),
            SeriesSamples::Int64 { samples, .. } => (samples[0].0, samples.len()),
            SeriesSamples::Histogram { samples } => (samples[0].0, samples.len()),
            SeriesSamples::ExponentialHistogram { samples } => (samples[0].0, samples.len()),
            SeriesSamples::Summary { samples } => (samples[0].0, samples.len()),
        };
        assert_eq!(timestamp_ms, 95_000);
        assert_eq!(sample_count, 1);
    }
}

#[test]
fn wal_replay_policy_boundaries_use_capture_time_not_source_timestamp() {
    let tempdir = tempfile::tempdir().unwrap();
    let wal_path = tempdir.path().join("wal-event-time-boundaries.log");
    let mut writer = WalWriter::create(&wal_path).unwrap();
    let points = [
        ("lower", 90_000),
        ("upper", 105_000),
        ("old", 89_999),
        ("future", 105_001),
        ("missing", 0),
    ]
    .into_iter()
    .map(|(case, timestamp_ms)| number_datapoint(case, timestamp_ms, NumberValue::Float(1.0)))
    .collect();
    writer
        .append_otlp_batch(&OtlpWalBatch {
            // If this diagnostic field were incorrectly used as the policy clock, no boundary
            // decision below would match the expected capture-time window.
            source_timestamp_ms: 9_999_999,
            captured_at_ms: 100_000,
            payload: request(vec![], vec![gauge_metric("policy.boundary", points)]).encode_to_vec(),
            ..wal_metadata(100_000)
        })
        .unwrap();
    writer.flush().unwrap();
    drop(writer);

    let mut head = test_head_with_window(Duration::from_secs(3_600));
    let mut labels = FlatInternedLabelSetStore::<DefaultSymbolTable>::default();
    let report = replay_wal_file_into_head(
        &wal_path,
        EventTimePolicy::new(TimeDelta::seconds(10), TimeDelta::seconds(5), true),
        &mut head,
        &mut labels,
    )
    .unwrap()
    .report;

    assert_eq!(report.policy_accepted_datapoints, 2);
    assert_eq!(report.dropped_too_old_datapoints, 1);
    assert_eq!(report.dropped_too_future_datapoints, 1);
    assert_eq!(report.missing_timestamp_datapoints, 1);
    assert_eq!(report.datapoints_replayed, 2);
    assert_eq!(labels.len(), 2);

    let results = head
        .query_selector(
            &labels,
            &SegmentSelector::with_metric("policy.boundary", vec![]),
            0,
            200_000,
        )
        .unwrap();
    assert_eq!(results.len(), 2);
    let mut timestamps = results
        .into_iter()
        .flat_map(|result| result.samples.into_iter().map(|sample| sample.0))
        .collect::<Vec<_>>();
    timestamps.sort_unstable();
    assert_eq!(timestamps, vec![90_000, 105_000]);
}

fn test_head() -> HeadBuffer {
    test_head_with_window(Duration::from_secs(10))
}

fn test_head_with_window(window_duration: Duration) -> HeadBuffer {
    HeadBuffer::new(HeadConfig::with_block_size(
        window_duration,
        2,
        FloatEncoding::Gorilla,
        IntEncoding::DeltaZigZag,
    ))
    .unwrap()
}

#[derive(Clone, Copy)]
enum NumberValue {
    Float(f64),
    Int(i64),
}

fn wal_metadata(captured_at_ms: i64) -> OtlpWalBatch {
    OtlpWalBatch {
        topic: "metrics".to_string(),
        partition: 0,
        offset: 1,
        source_timestamp_ms: captured_at_ms,
        captured_at_ms,
        payload: Vec::new(),
    }
}

fn policy_timestamps() -> [(&'static str, u64); 4] {
    [
        ("accepted", 95_000),
        ("old", 89_999),
        ("future", 105_001),
        ("missing", 0),
    ]
}

fn cumulative_histogram_metric(
    metric_name: &str,
    timestamp_ms: u64,
    start_time_ms: u64,
    count: u64,
    sum: f64,
    bucket_counts: Vec<u64>,
) -> tonic::metrics::v1::Metric {
    tonic::metrics::v1::Metric {
        name: metric_name.to_string(),
        data: Some(tonic::metrics::v1::metric::Data::Histogram(
            tonic::metrics::v1::Histogram {
                data_points: vec![tonic::metrics::v1::HistogramDataPoint {
                    attributes: vec![kv("instance", "one")],
                    start_time_unix_nano: start_time_ms * 1_000_000,
                    time_unix_nano: timestamp_ms * 1_000_000,
                    count,
                    sum: Some(sum),
                    bucket_counts,
                    explicit_bounds: vec![1.0],
                    ..Default::default()
                }],
                aggregation_temporality: tonic::metrics::v1::AggregationTemporality::Cumulative
                    as i32,
            },
        )),
        ..Default::default()
    }
}

fn cumulative_exponential_histogram_metric(
    metric_name: &str,
    timestamp_ms: u64,
    start_time_ms: u64,
    count: u64,
    sum: f64,
    zero_count: u64,
    bucket_counts: Vec<u64>,
) -> tonic::metrics::v1::Metric {
    tonic::metrics::v1::Metric {
        name: metric_name.to_string(),
        data: Some(tonic::metrics::v1::metric::Data::ExponentialHistogram(
            tonic::metrics::v1::ExponentialHistogram {
                data_points: vec![tonic::metrics::v1::ExponentialHistogramDataPoint {
                    attributes: vec![kv("instance", "one")],
                    start_time_unix_nano: start_time_ms * 1_000_000,
                    time_unix_nano: timestamp_ms * 1_000_000,
                    count,
                    sum: Some(sum),
                    scale: 1,
                    zero_count,
                    positive: Some(
                        tonic::metrics::v1::exponential_histogram_data_point::Buckets {
                            offset: 0,
                            bucket_counts,
                        },
                    ),
                    ..Default::default()
                }],
                aggregation_temporality: tonic::metrics::v1::AggregationTemporality::Cumulative
                    as i32,
            },
        )),
        ..Default::default()
    }
}

fn number_datapoint(
    case: &str,
    timestamp_ms: u64,
    value: NumberValue,
) -> tonic::metrics::v1::NumberDataPoint {
    tonic::metrics::v1::NumberDataPoint {
        attributes: vec![kv("case", case)],
        time_unix_nano: timestamp_ms * 1_000_000,
        value: Some(match value {
            NumberValue::Float(value) => {
                tonic::metrics::v1::number_data_point::Value::AsDouble(value)
            }
            NumberValue::Int(value) => tonic::metrics::v1::number_data_point::Value::AsInt(value),
        }),
        ..Default::default()
    }
}

fn gauge_metric(
    metric_name: &str,
    data_points: Vec<tonic::metrics::v1::NumberDataPoint>,
) -> tonic::metrics::v1::Metric {
    tonic::metrics::v1::Metric {
        name: metric_name.to_string(),
        data: Some(tonic::metrics::v1::metric::Data::Gauge(
            tonic::metrics::v1::Gauge { data_points },
        )),
        ..Default::default()
    }
}

fn policy_gauge_metric() -> tonic::metrics::v1::Metric {
    gauge_metric(
        "policy.gauge",
        policy_timestamps()
            .into_iter()
            .map(|(case, timestamp_ms)| {
                number_datapoint(case, timestamp_ms, NumberValue::Float(1.0))
            })
            .collect(),
    )
}

fn policy_sum_metric() -> tonic::metrics::v1::Metric {
    let data_points = policy_timestamps()
        .into_iter()
        .map(|(case, timestamp_ms)| number_datapoint(case, timestamp_ms, NumberValue::Int(1)))
        .collect();
    tonic::metrics::v1::Metric {
        name: "policy.sum".to_string(),
        data: Some(tonic::metrics::v1::metric::Data::Sum(
            tonic::metrics::v1::Sum {
                data_points,
                aggregation_temporality: tonic::metrics::v1::AggregationTemporality::Cumulative
                    as i32,
                is_monotonic: true,
            },
        )),
        ..Default::default()
    }
}

fn policy_histogram_metric() -> tonic::metrics::v1::Metric {
    let data_points = policy_timestamps()
        .into_iter()
        .map(
            |(case, timestamp_ms)| tonic::metrics::v1::HistogramDataPoint {
                attributes: vec![kv("case", case)],
                time_unix_nano: timestamp_ms * 1_000_000,
                count: 1,
                bucket_counts: vec![1],
                ..Default::default()
            },
        )
        .collect();
    tonic::metrics::v1::Metric {
        name: "policy.histogram".to_string(),
        data: Some(tonic::metrics::v1::metric::Data::Histogram(
            tonic::metrics::v1::Histogram {
                data_points,
                aggregation_temporality: tonic::metrics::v1::AggregationTemporality::Cumulative
                    as i32,
            },
        )),
        ..Default::default()
    }
}

fn policy_exponential_histogram_metric() -> tonic::metrics::v1::Metric {
    let data_points = policy_timestamps()
        .into_iter()
        .map(
            |(case, timestamp_ms)| tonic::metrics::v1::ExponentialHistogramDataPoint {
                attributes: vec![kv("case", case)],
                time_unix_nano: timestamp_ms * 1_000_000,
                count: 1,
                zero_count: 1,
                ..Default::default()
            },
        )
        .collect();
    tonic::metrics::v1::Metric {
        name: "policy.exponential_histogram".to_string(),
        data: Some(tonic::metrics::v1::metric::Data::ExponentialHistogram(
            tonic::metrics::v1::ExponentialHistogram {
                data_points,
                aggregation_temporality: tonic::metrics::v1::AggregationTemporality::Cumulative
                    as i32,
            },
        )),
        ..Default::default()
    }
}

fn policy_summary_metric() -> tonic::metrics::v1::Metric {
    let data_points = policy_timestamps()
        .into_iter()
        .map(
            |(case, timestamp_ms)| tonic::metrics::v1::SummaryDataPoint {
                attributes: vec![kv("case", case)],
                time_unix_nano: timestamp_ms * 1_000_000,
                count: 1,
                sum: 1.0,
                ..Default::default()
            },
        )
        .collect();
    tonic::metrics::v1::Metric {
        name: "policy.summary".to_string(),
        data: Some(tonic::metrics::v1::metric::Data::Summary(
            tonic::metrics::v1::Summary { data_points },
        )),
        ..Default::default()
    }
}

fn number_metric(
    metric_name: &str,
    pod_name: &str,
    timestamp_ms: u64,
    value: NumberValue,
) -> tonic::metrics::v1::Metric {
    tonic::metrics::v1::Metric {
        name: metric_name.to_string(),
        data: Some(tonic::metrics::v1::metric::Data::Gauge(
            tonic::metrics::v1::Gauge {
                data_points: vec![tonic::metrics::v1::NumberDataPoint {
                    attributes: vec![kv("pod.name", pod_name)],
                    time_unix_nano: timestamp_ms * 1_000_000,
                    value: Some(match value {
                        NumberValue::Float(value) => {
                            tonic::metrics::v1::number_data_point::Value::AsDouble(value)
                        }
                        NumberValue::Int(value) => {
                            tonic::metrics::v1::number_data_point::Value::AsInt(value)
                        }
                    }),
                    ..Default::default()
                }],
            },
        )),
        ..Default::default()
    }
}

fn request(
    resource_attrs: Vec<tonic::common::v1::KeyValue>,
    metrics: Vec<tonic::metrics::v1::Metric>,
) -> tonic::collector::metrics::v1::ExportMetricsServiceRequest {
    tonic::collector::metrics::v1::ExportMetricsServiceRequest {
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

fn kv(key: &str, value: &str) -> tonic::common::v1::KeyValue {
    tonic::common::v1::KeyValue {
        key: key.to_string(),
        value: Some(tonic::common::v1::AnyValue {
            value: Some(tonic::common::v1::any_value::Value::StringValue(
                value.to_string(),
            )),
        }),
        key_strindex: 0,
    }
}
