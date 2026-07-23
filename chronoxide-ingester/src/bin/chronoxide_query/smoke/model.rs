use super::*;

#[derive(Debug, Clone, Default, PartialEq)]
pub(in super::super) struct QueryReadbackVerification {
    pub(in super::super) checked_queries: usize,
    pub(in super::super) mismatches: Vec<QueryReadbackMismatch>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(in super::super) struct QuerySmokeDiagnostics {
    pub(in super::super) store_open: Duration,
    pub(in super::super) smoke_verify: Duration,
    pub(in super::super) readback: Option<QueryReadbackDiagnostics>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(in super::super) struct QueryReadbackDiagnostics {
    pub(in super::super) collect_expected_readbacks: Duration,
    pub(in super::super) store_open: Duration,
    pub(in super::super) query_session_open: Duration,
    pub(in super::super) promql_queries: Duration,
    pub(in super::super) expected_queries: usize,
    pub(in super::super) executed_queries: usize,
    pub(in super::super) skipped_queries: usize,
    pub(in super::super) isolation_check_skips: usize,
    pub(in super::super) multi_step_range_expected_queries: usize,
    pub(in super::super) multi_step_range_executed_queries: usize,
    pub(in super::super) multi_step_range_skipped_queries: usize,
    pub(in super::super) skip_reasons: BTreeMap<String, usize>,
    pub(in super::super) session_stats: SegmentStoreQuerySessionStats,
    pub(in super::super) session_profile: SegmentStoreQueryProfile,
}

#[derive(Debug, Clone, PartialEq)]
pub(in super::super) struct QueryReadbackMismatch {
    pub(in super::super) query: String,
    pub(in super::super) missing_expected_samples: Vec<(u64, f64)>,
    pub(in super::super) actual_samples: Vec<(u64, f64)>,
}

#[derive(Debug, Clone, PartialEq)]
pub(in super::super) struct ExpectedReadback {
    pub(in super::super) query: String,
    pub(in super::super) start_ms: u64,
    pub(in super::super) end_ms: u64,
    pub(in super::super) step_ms: Option<u64>,
    pub(in super::super) samples: Vec<(u64, f64)>,
    pub(in super::super) isolation_check: Option<ReadbackIsolationCheck>,
}

#[derive(Debug, Clone, PartialEq)]
pub(in super::super) struct ReadbackIsolationCheck {
    pub(in super::super) query: String,
    pub(in super::super) start_ms: u64,
    pub(in super::super) end_ms: u64,
    pub(in super::super) samples: Vec<(u64, f64)>,
    pub(in super::super) failure_reason: String,
}

impl ExpectedReadback {
    pub(super) fn isolation_check(&self) -> ReadbackIsolationCheck {
        self.isolation_check_with_reason(
            "exact selector did not isolate the independently decoded physical series",
        )
    }

    pub(super) fn isolation_check_with_reason(
        &self,
        failure_reason: &str,
    ) -> ReadbackIsolationCheck {
        ReadbackIsolationCheck {
            query: self.query.clone(),
            start_ms: self.start_ms,
            end_ms: self.end_ms,
            samples: self.samples.clone(),
            failure_reason: failure_reason.to_string(),
        }
    }
}
