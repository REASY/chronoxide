use chrono::TimeDelta;

/// Applies the ingest-time acceptance window to an OTLP datapoint timestamp.
///
/// Datapoint timestamps are event time. `captured_at_ms` is the trusted policy
/// clock supplied by the caller for either live ingestion or deterministic
/// replay.
#[derive(Debug, Default, Clone, Copy, Eq, PartialEq)]
pub struct EventTimePolicy {
    // event_ms must be >= captured_at_ms - max_event_age_ms.
    max_event_age_ms: i64,
    // event_ms must be <= captured_at_ms + max_event_lead_ms.
    max_event_lead_ms: i64,
    drop_outdated: bool,
}

impl EventTimePolicy {
    pub fn new(max_event_age: TimeDelta, max_event_lead: TimeDelta, drop_outdated: bool) -> Self {
        assert!(
            max_event_age >= TimeDelta::zero(),
            "max_event_age must be non-negative"
        );
        assert!(
            max_event_lead >= TimeDelta::zero(),
            "max_event_lead must be non-negative"
        );
        Self {
            max_event_age_ms: max_event_age.num_milliseconds(),
            max_event_lead_ms: max_event_lead.num_milliseconds(),
            drop_outdated,
        }
    }

    /// Evaluates one OTLP nanosecond timestamp against a trusted millisecond clock.
    ///
    /// Only the exact OTLP missing-timestamp value (`0`) is rejected as missing.
    /// Non-zero sub-millisecond timestamps remain valid and truncate to epoch
    /// millisecond zero, matching storage precision.
    pub fn evaluate(&self, time_unix_nano: u64, captured_at_ms: i64) -> DatapointTimeEvaluation {
        if time_unix_nano == 0 {
            return DatapointTimeEvaluation {
                decision: DatapointTimeDecision::MissingTimestamp,
                skew_ms: None,
            };
        }

        let event_ms = time_unix_nano / 1_000_000;
        let event_ms_i128 = i128::from(event_ms);
        let captured_at_ms_i128 = i128::from(captured_at_ms);
        let skew_ms = Some(saturating_i128_to_i64(event_ms_i128 - captured_at_ms_i128));

        if !self.drop_outdated {
            return DatapointTimeEvaluation {
                decision: DatapointTimeDecision::Accepted(event_ms),
                skew_ms,
            };
        }

        let min_event_ms = captured_at_ms_i128 - i128::from(self.max_event_age_ms);
        if event_ms_i128 < min_event_ms {
            return DatapointTimeEvaluation {
                decision: DatapointTimeDecision::DroppedTooOld,
                skew_ms,
            };
        }

        let max_event_ms = captured_at_ms_i128 + i128::from(self.max_event_lead_ms);
        if event_ms_i128 > max_event_ms {
            return DatapointTimeEvaluation {
                decision: DatapointTimeDecision::DroppedTooFuture,
                skew_ms,
            };
        }

        DatapointTimeEvaluation {
            decision: DatapointTimeDecision::Accepted(event_ms),
            skew_ms,
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum DatapointTimeDecision {
    Accepted(u64),
    DroppedTooOld,
    DroppedTooFuture,
    MissingTimestamp,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct DatapointTimeEvaluation {
    pub decision: DatapointTimeDecision,
    pub skew_ms: Option<i64>,
}

fn saturating_i128_to_i64(value: i128) -> i64 {
    if value > i128::from(i64::MAX) {
        i64::MAX
    } else if value < i128::from(i64::MIN) {
        i64::MIN
    } else {
        value as i64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn strict_policy(age_ms: i64, lead_ms: i64) -> EventTimePolicy {
        EventTimePolicy::new(
            TimeDelta::milliseconds(age_ms),
            TimeDelta::milliseconds(lead_ms),
            true,
        )
    }

    fn timestamp_ms(ms: u64) -> u64 {
        ms.checked_mul(1_000_000).expect("test timestamp fits")
    }

    #[test]
    fn exact_zero_is_missing_without_a_skew_sample() {
        let policy = strict_policy(1_000, 1_000);

        for captured_at_ms in [i64::MIN, -1, 0, 1, i64::MAX] {
            assert_eq!(
                policy.evaluate(0, captured_at_ms),
                DatapointTimeEvaluation {
                    decision: DatapointTimeDecision::MissingTimestamp,
                    skew_ms: None,
                }
            );
        }
    }

    #[test]
    fn nonzero_sub_millisecond_timestamps_are_valid_epoch_zero_events() {
        let policy = strict_policy(0, 0);

        for time_unix_nano in [1, 999_999] {
            assert_eq!(
                policy.evaluate(time_unix_nano, 0),
                DatapointTimeEvaluation {
                    decision: DatapointTimeDecision::Accepted(0),
                    skew_ms: Some(0),
                }
            );
        }
        assert_eq!(
            policy.evaluate(1_000_000, 0).decision,
            DatapointTimeDecision::DroppedTooFuture
        );
    }

    #[test]
    fn age_and_lead_boundaries_are_inclusive() {
        let policy = strict_policy(1_000, 500);
        let captured_at_ms = 10_000;

        let cases = [
            (8_999, DatapointTimeDecision::DroppedTooOld),
            (9_000, DatapointTimeDecision::Accepted(9_000)),
            (10_000, DatapointTimeDecision::Accepted(10_000)),
            (10_500, DatapointTimeDecision::Accepted(10_500)),
            (10_501, DatapointTimeDecision::DroppedTooFuture),
        ];
        for (event_ms, expected) in cases {
            assert_eq!(
                policy
                    .evaluate(timestamp_ms(event_ms), captured_at_ms)
                    .decision,
                expected
            );
        }
    }

    #[test]
    fn skew_is_reported_for_accepted_and_dropped_datapoints() {
        let policy = strict_policy(10, 20);

        assert_eq!(policy.evaluate(timestamp_ms(989), 1_000).skew_ms, Some(-11));
        assert_eq!(policy.evaluate(timestamp_ms(990), 1_000).skew_ms, Some(-10));
        assert_eq!(
            policy.evaluate(timestamp_ms(1_020), 1_000).skew_ms,
            Some(20)
        );
        assert_eq!(
            policy.evaluate(timestamp_ms(1_021), 1_000).skew_ms,
            Some(21)
        );
    }

    #[test]
    fn disabled_dropping_accepts_every_nonzero_timestamp() {
        let policy = EventTimePolicy::new(TimeDelta::zero(), TimeDelta::zero(), false);

        for (time_unix_nano, captured_at_ms, expected_ms) in [
            (1, i64::MAX, 0),
            (1_000_000, i64::MIN, 1),
            (u64::MAX, 0, u64::MAX / 1_000_000),
        ] {
            assert_eq!(
                policy.evaluate(time_unix_nano, captured_at_ms).decision,
                DatapointTimeDecision::Accepted(expected_ms)
            );
        }
    }

    #[test]
    fn default_policy_accepts_nonzero_timestamps_and_rejects_zero() {
        let policy = EventTimePolicy::default();

        assert_eq!(
            policy.evaluate(0, 123).decision,
            DatapointTimeDecision::MissingTimestamp
        );
        assert_eq!(
            policy.evaluate(timestamp_ms(456), 123).decision,
            DatapointTimeDecision::Accepted(456)
        );
    }

    #[test]
    fn extreme_skew_saturates_without_overflow() {
        let policy = EventTimePolicy::default();

        assert_eq!(
            policy.evaluate(u64::MAX, i64::MIN),
            DatapointTimeEvaluation {
                decision: DatapointTimeDecision::Accepted(u64::MAX / 1_000_000),
                skew_ms: Some(i64::MAX),
            }
        );
        assert_eq!(policy.evaluate(1, i64::MAX).skew_ms, Some(-i64::MAX));
        assert_eq!(saturating_i128_to_i64(i128::MIN), i64::MIN);
        assert_eq!(saturating_i128_to_i64(i128::MAX), i64::MAX);
    }

    #[test]
    fn maximum_windows_do_not_overflow_policy_bounds() {
        let policy = EventTimePolicy::new(TimeDelta::MAX, TimeDelta::MAX, true);

        assert_eq!(
            policy.evaluate(1, i64::MAX).decision,
            DatapointTimeDecision::Accepted(0)
        );
        assert_eq!(
            policy.evaluate(u64::MAX, i64::MAX).decision,
            DatapointTimeDecision::Accepted(u64::MAX / 1_000_000)
        );
        assert_eq!(
            policy.evaluate(1, i64::MIN).decision,
            DatapointTimeDecision::DroppedTooFuture
        );
    }

    #[test]
    #[should_panic(expected = "max_event_age must be non-negative")]
    fn negative_age_is_rejected() {
        let _ = EventTimePolicy::new(TimeDelta::milliseconds(-1), TimeDelta::zero(), true);
    }

    #[test]
    #[should_panic(expected = "max_event_lead must be non-negative")]
    fn negative_lead_is_rejected() {
        let _ = EventTimePolicy::new(TimeDelta::zero(), TimeDelta::milliseconds(-1), true);
    }
}
