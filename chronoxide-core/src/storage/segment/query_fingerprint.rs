use std::fmt;

use sha2::{Digest, Sha256};

use super::{CounterResetHint, QueryExecution, QueryResultTemporality, SegmentQueryResult};

const QUERY_EXECUTION_FINGERPRINT_DOMAIN: &[u8] = b"chronoxide/query-execution-fingerprint";
const PORTABLE_QUERY_RESULT_FINGERPRINT_DOMAIN: &[u8] =
    b"chronoxide/portable-query-result-fingerprint";

pub const QUERY_EXECUTION_FINGERPRINT_VERSION: u16 = 1;
pub const PORTABLE_QUERY_RESULT_FINGERPRINT_VERSION: u16 = 1;

#[derive(Debug, Clone, PartialEq)]
pub struct PortableQuerySeries {
    pub labels: Vec<(String, String)>,
    pub samples: Vec<(u64, f64)>,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub struct QueryExecutionFingerprint([u8; 32]);

impl QueryExecutionFingerprint {
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    pub fn to_hex(self) -> String {
        let mut output = String::with_capacity(64);
        for byte in self.0 {
            use std::fmt::Write as _;
            write!(&mut output, "{byte:02x}").expect("writing to String cannot fail");
        }
        output
    }
}

impl fmt::Display for QueryExecutionFingerprint {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in self.0 {
            write!(formatter, "{byte:02x}")?;
        }
        Ok(())
    }
}

impl QueryExecution {
    pub fn semantic_fingerprint_sha256(&self) -> QueryExecutionFingerprint {
        let mut digest = Sha256::new();
        digest.update(QUERY_EXECUTION_FINGERPRINT_DOMAIN);
        digest.update(QUERY_EXECUTION_FINGERPRINT_VERSION.to_le_bytes());
        update_u64(&mut digest, self.results.len() as u64);
        for result in &self.results {
            update_result(&mut digest, result);
        }
        QueryExecutionFingerprint(digest.finalize().into())
    }

    pub fn portable_semantic_fingerprint_sha256(&self) -> QueryExecutionFingerprint {
        portable_fingerprint_from_encoded(
            self.results
                .iter()
                .map(encode_portable_query_result)
                .collect(),
        )
    }
}

pub fn portable_query_result_fingerprint_sha256(
    series: Vec<PortableQuerySeries>,
) -> QueryExecutionFingerprint {
    portable_fingerprint_from_encoded(series.into_iter().map(encode_portable_series).collect())
}

fn portable_fingerprint_from_encoded(
    mut encoded_series: Vec<Vec<u8>>,
) -> QueryExecutionFingerprint {
    encoded_series.sort_unstable();

    let mut digest = Sha256::new();
    digest.update(PORTABLE_QUERY_RESULT_FINGERPRINT_DOMAIN);
    digest.update(PORTABLE_QUERY_RESULT_FINGERPRINT_VERSION.to_le_bytes());
    update_u64(&mut digest, encoded_series.len() as u64);
    for encoded in encoded_series {
        update_bytes(&mut digest, &encoded);
    }
    QueryExecutionFingerprint(digest.finalize().into())
}

fn encode_portable_query_result(result: &SegmentQueryResult) -> Vec<u8> {
    encode_portable_parts(result.labels.pairs(), result.samples.iter().copied())
}

fn encode_portable_series(series: PortableQuerySeries) -> Vec<u8> {
    encode_portable_parts(
        series
            .labels
            .iter()
            .map(|(key, value)| (key.as_str(), value.as_str())),
        series.samples.iter().copied(),
    )
}

fn encode_portable_parts<'a>(
    labels: impl Iterator<Item = (&'a str, &'a str)>,
    samples: impl Iterator<Item = (u64, f64)>,
) -> Vec<u8> {
    let mut labels = labels.collect::<Vec<_>>();
    labels.sort_unstable();
    let mut samples = samples.collect::<Vec<_>>();
    samples.sort_unstable_by_key(|(timestamp_ms, value)| (*timestamp_ms, value.to_bits()));
    let mut encoded = Vec::new();
    encoded.extend_from_slice(&(labels.len() as u64).to_le_bytes());
    for (key, value) in labels {
        encode_bytes(&mut encoded, key.as_bytes());
        encode_bytes(&mut encoded, value.as_bytes());
    }
    encoded.extend_from_slice(&(samples.len() as u64).to_le_bytes());
    for (timestamp_ms, value) in samples {
        encoded.extend_from_slice(&timestamp_ms.to_le_bytes());
        encoded.extend_from_slice(&value.to_bits().to_le_bytes());
    }
    encoded
}

fn encode_bytes(output: &mut Vec<u8>, bytes: &[u8]) {
    output.extend_from_slice(&(bytes.len() as u64).to_le_bytes());
    output.extend_from_slice(bytes);
}

fn update_result(digest: &mut Sha256, result: &SegmentQueryResult) {
    update_u64(digest, result.series_id);

    update_u64(digest, result.labels.len() as u64);
    for (key, value) in result.labels.pairs() {
        update_bytes(digest, key.as_bytes());
        update_bytes(digest, value.as_bytes());
    }

    update_u64(digest, result.samples.len() as u64);
    for &(timestamp_ms, value) in &result.samples {
        update_u64(digest, timestamp_ms);
        update_u64(digest, value.to_bits());
    }

    update_u64(digest, result.counter_reset_hints.len() as u64);
    for reset_hint in &result.counter_reset_hints {
        digest.update([counter_reset_hint_discriminant(*reset_hint)]);
    }

    update_u64(digest, result.sample_start_times.len() as u64);
    for start_time_ms in &result.sample_start_times {
        match start_time_ms {
            None => digest.update([0]),
            Some(start_time_ms) => {
                digest.update([1]);
                update_u64(digest, *start_time_ms);
            }
        }
    }

    digest.update([temporality_discriminant(result.temporality)]);
}

fn update_bytes(digest: &mut Sha256, bytes: &[u8]) {
    update_u64(digest, bytes.len() as u64);
    digest.update(bytes);
}

fn update_u64(digest: &mut Sha256, value: u64) {
    digest.update(value.to_le_bytes());
}

fn counter_reset_hint_discriminant(reset_hint: CounterResetHint) -> u8 {
    match reset_hint {
        CounterResetHint::Unknown => 0,
        CounterResetHint::CounterReset => 1,
        CounterResetHint::NotCounterReset => 2,
        CounterResetHint::GaugeType => 3,
    }
}

fn temporality_discriminant(temporality: QueryResultTemporality) -> u8 {
    match temporality {
        QueryResultTemporality::Unknown => 0,
        QueryResultTemporality::Cumulative => 1,
        QueryResultTemporality::Delta => 2,
        QueryResultTemporality::Mixed => 3,
    }
}

#[cfg(test)]
mod tests {
    use super::super::*;

    fn result_with_one_sample(value_bits: u64) -> SegmentQueryResult {
        SegmentQueryResult {
            series_id: 7,
            labels: shared_query_labels(vec![(
                "__name__".to_string(),
                "fingerprint_metric".to_string(),
            )]),
            samples: vec![(1_000, f64::from_bits(value_bits))],
            counter_reset_hints: Vec::new(),
            sample_start_times: Vec::new(),
            temporality: QueryResultTemporality::Unknown,
            labels_complete: true,
            metric_name_dropped_series_id: None,
            delta_projection_intervals: Vec::new(),
        }
    }

    fn execution_with_one_sample(value_bits: u64) -> QueryExecution {
        QueryExecution {
            results: vec![result_with_one_sample(value_bits)],
            stats: QueryStats::default(),
        }
    }

    fn metadata_fingerprints(
        reset_hints: &[CounterResetHint],
        start_times: &[Option<u64>],
        temporalities: &[QueryResultTemporality],
    ) -> Vec<QueryExecutionFingerprint> {
        let base = execution_with_one_sample(1.0_f64.to_bits());
        let mut fingerprints = Vec::new();
        for &reset_hint in reset_hints {
            for &start_time in start_times {
                for &temporality in temporalities {
                    let mut execution = base.clone();
                    execution.results[0].counter_reset_hints = vec![reset_hint];
                    execution.results[0].sample_start_times = vec![start_time];
                    execution.results[0].temporality = temporality;
                    fingerprints.push(execution.semantic_fingerprint_sha256());
                }
            }
        }
        fingerprints
    }

    #[test]
    fn query_execution_semantic_fingerprint_has_stable_versioned_digest() {
        let execution = QueryExecution {
            results: vec![
                SegmentQueryResult {
                    series_id: 7,
                    labels: shared_query_labels(vec![
                        ("__name__".to_string(), "fingerprint_metric".to_string()),
                        ("zone".to_string(), "sg-1".to_string()),
                    ]),
                    samples: vec![(1_000, 0.0), (1_001, prometheus_stale_nan())],
                    counter_reset_hints: vec![
                        CounterResetHint::Unknown,
                        CounterResetHint::CounterReset,
                    ],
                    sample_start_times: vec![None, Some(900)],
                    temporality: QueryResultTemporality::Delta,
                    labels_complete: true,
                    metric_name_dropped_series_id: None,
                    delta_projection_intervals: Vec::new(),
                },
                SegmentQueryResult {
                    series_id: 9,
                    labels: shared_query_labels(vec![(
                        "__name__".to_string(),
                        "other".to_string(),
                    )]),
                    samples: vec![(2_000, f64::from_bits(0x7ff8_0000_0000_0042))],
                    counter_reset_hints: vec![CounterResetHint::GaugeType],
                    sample_start_times: Vec::new(),
                    temporality: QueryResultTemporality::Mixed,
                    labels_complete: true,
                    metric_name_dropped_series_id: None,
                    delta_projection_intervals: Vec::new(),
                },
            ],
            stats: QueryStats {
                segments_considered: 99,
                bytes_read: 123_456,
                ..QueryStats::default()
            },
        };

        let fingerprint = execution.semantic_fingerprint_sha256();

        assert_eq!(QUERY_EXECUTION_FINGERPRINT_VERSION, 1);
        assert_eq!(
            fingerprint.to_hex(),
            "1e3600355c57e1d1eb2fcb806f2af584a0569032a5c684f7f8a3a4f382e01aea"
        );
        assert_eq!(format!("{fingerprint}"), fingerprint.to_hex());
        assert_eq!(fingerprint.as_bytes().len(), 32);
    }

    #[test]
    fn query_execution_semantic_fingerprint_preserves_returned_series_order() {
        let mut left = execution_with_one_sample(1.0_f64.to_bits());
        let mut second = result_with_one_sample(2.0_f64.to_bits());
        second.series_id = 8;
        left.results.push(second);
        let mut right = left.clone();
        right.results.reverse();

        assert_ne!(
            left.semantic_fingerprint_sha256(),
            right.semantic_fingerprint_sha256()
        );
    }

    #[test]
    fn query_execution_semantic_fingerprint_preserves_returned_label_order() {
        let mut left = execution_with_one_sample(1.0_f64.to_bits());
        left.results[0].labels = shared_query_labels(vec![
            ("alpha".to_string(), "one".to_string()),
            ("beta".to_string(), "two".to_string()),
        ]);
        let mut right = left.clone();
        right.results[0].labels = shared_query_labels(vec![
            ("beta".to_string(), "two".to_string()),
            ("alpha".to_string(), "one".to_string()),
        ]);

        assert_ne!(
            left.semantic_fingerprint_sha256(),
            right.semantic_fingerprint_sha256()
        );
    }

    #[test]
    fn query_execution_semantic_fingerprint_distinguishes_signed_zero() {
        let positive = execution_with_one_sample(0.0_f64.to_bits());
        let negative = execution_with_one_sample((-0.0_f64).to_bits());

        assert_ne!(
            positive.semantic_fingerprint_sha256(),
            negative.semantic_fingerprint_sha256()
        );
    }

    #[test]
    fn query_execution_semantic_fingerprint_distinguishes_nan_payloads() {
        let ordinary = execution_with_one_sample(0x7ff8_0000_0000_0042);
        let stale = execution_with_one_sample(prometheus_stale_nan().to_bits());

        assert_ne!(
            ordinary.semantic_fingerprint_sha256(),
            stale.semantic_fingerprint_sha256()
        );
    }

    #[test]
    fn query_execution_semantic_fingerprint_hashes_raw_reset_vector_length() {
        let empty = execution_with_one_sample(1.0_f64.to_bits());
        let mut populated = empty.clone();
        populated.results[0].counter_reset_hints = vec![CounterResetHint::Unknown];

        assert_ne!(
            empty.semantic_fingerprint_sha256(),
            populated.semantic_fingerprint_sha256()
        );
    }

    #[test]
    fn query_execution_semantic_fingerprint_distinguishes_every_reset_hint() {
        let fingerprints = metadata_fingerprints(
            &[
                CounterResetHint::Unknown,
                CounterResetHint::CounterReset,
                CounterResetHint::NotCounterReset,
                CounterResetHint::GaugeType,
            ],
            &[None],
            &[QueryResultTemporality::Unknown],
        );

        for (index, fingerprint) in fingerprints.iter().enumerate() {
            assert!(!fingerprints[..index].contains(fingerprint));
        }
    }

    #[test]
    fn query_execution_semantic_fingerprint_hashes_raw_start_time_vector_length() {
        let empty = execution_with_one_sample(1.0_f64.to_bits());
        let mut populated = empty.clone();
        populated.results[0].sample_start_times = vec![None];

        assert_ne!(
            empty.semantic_fingerprint_sha256(),
            populated.semantic_fingerprint_sha256()
        );
    }

    #[test]
    fn query_execution_semantic_fingerprint_hashes_start_time_presence_and_value() {
        let base = execution_with_one_sample(1.0_f64.to_bits());
        let mut absent = base.clone();
        absent.results[0].sample_start_times = vec![None];
        let mut present = base.clone();
        present.results[0].sample_start_times = vec![Some(1_234)];
        let mut changed_value = base;
        changed_value.results[0].sample_start_times = vec![Some(1_235)];

        assert_ne!(
            absent.semantic_fingerprint_sha256(),
            present.semantic_fingerprint_sha256()
        );
        assert_ne!(
            present.semantic_fingerprint_sha256(),
            changed_value.semantic_fingerprint_sha256()
        );
    }

    #[test]
    fn query_execution_semantic_fingerprint_distinguishes_every_temporality() {
        let fingerprints = metadata_fingerprints(
            &[CounterResetHint::Unknown],
            &[None],
            &[
                QueryResultTemporality::Unknown,
                QueryResultTemporality::Cumulative,
                QueryResultTemporality::Delta,
                QueryResultTemporality::Mixed,
            ],
        );

        for (index, fingerprint) in fingerprints.iter().enumerate() {
            assert!(!fingerprints[..index].contains(fingerprint));
        }
    }

    #[test]
    fn query_execution_semantic_fingerprint_ignores_query_stats() {
        let left = execution_with_one_sample(1.0_f64.to_bits());
        let mut right = left.clone();
        right.stats.segments_considered = 1;
        right.stats.bytes_read = 2;
        right.stats.samples_decoded = 3;

        assert_eq!(
            left.semantic_fingerprint_sha256(),
            right.semantic_fingerprint_sha256()
        );
    }

    #[test]
    fn query_fingerprints_leave_compact_compatibility_labels_unmaterialized() {
        let mut owned = execution_with_one_sample(1.0_f64.to_bits());
        owned.results[0].labels = shared_query_labels(vec![
            ("zone".to_string(), "sg-1".to_string()),
            ("__name__".to_string(), "fingerprint_metric".to_string()),
        ]);
        let expected_semantic = owned.semantic_fingerprint_sha256();
        let expected_portable =
            portable_query_result_fingerprint_sha256(vec![PortableQuerySeries {
                labels: owned.results[0].labels.to_vec(),
                samples: owned.results[0].samples.clone(),
            }]);

        let compact_labels = {
            let mut interner = QueryLabelInterner::default();
            interner.set_policy(QueryLabelStoragePolicy::CompactIds);
            interner
                .try_intern_labels(owned.results[0].labels.to_vec())
                .expect("compact labels fit the default governed arena")
        };
        assert_eq!(
            compact_labels.compact_ids_compatibility_view_materialized_for_test(),
            Some(false)
        );
        let mut compact = owned;
        compact.results[0].labels = compact_labels;

        assert_eq!(compact.semantic_fingerprint_sha256(), expected_semantic);
        assert_eq!(
            compact.results[0]
                .labels
                .compact_ids_compatibility_view_materialized_for_test(),
            Some(false)
        );
        assert_eq!(
            compact.portable_semantic_fingerprint_sha256(),
            expected_portable
        );
        assert_eq!(
            compact.results[0]
                .labels
                .compact_ids_compatibility_view_materialized_for_test(),
            Some(false)
        );
    }

    #[test]
    fn portable_fingerprint_canonicalizes_series_labels_and_samples() {
        let left = vec![
            PortableQuerySeries {
                labels: vec![("b".to_string(), "2".to_string())],
                samples: vec![(2_000, 2.0), (1_000, 1.0)],
            },
            PortableQuerySeries {
                labels: vec![
                    ("z".to_string(), "9".to_string()),
                    ("a".to_string(), "1".to_string()),
                ],
                samples: vec![(3_000, -0.0)],
            },
        ];
        let right = vec![
            PortableQuerySeries {
                labels: vec![
                    ("a".to_string(), "1".to_string()),
                    ("z".to_string(), "9".to_string()),
                ],
                samples: vec![(3_000, -0.0)],
            },
            PortableQuerySeries {
                labels: vec![("b".to_string(), "2".to_string())],
                samples: vec![(1_000, 1.0), (2_000, 2.0)],
            },
        ];

        assert_eq!(
            portable_query_result_fingerprint_sha256(left),
            portable_query_result_fingerprint_sha256(right)
        );
    }
}
