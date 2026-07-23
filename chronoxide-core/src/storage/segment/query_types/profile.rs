use super::super::{BTreeSet, Duration};
use super::labels::QueryLabels;
use super::store::SegmentReader;
use crate::storage::index::SegmentIndexReadStats;
use crate::storage::symbols::{SegmentSymbolReadStats, SegmentSymbolResourceSnapshot};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SegmentStoreQuerySessionStats {
    pub index_routing_opens: u64,
    pub segment_context_opens: u64,
    pub symbols_bin_opens: u64,
    pub indexes_puffin_opens: u64,
    pub series_bin_opens: u64,
    pub chunk_index_bin_opens: u64,
    pub chunks_bin_opens: u64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ChunkPayloadLocalityProfile {
    pub reads: u64,
    pub forward_gaps: u64,
    pub forward_gap_bytes: u64,
    pub backward_jumps: u64,
    pub contiguous_runs: u64,
    pub contiguous_span_bytes: u64,
    pub coalesced_4k_runs: u64,
    pub coalesced_4k_span_bytes: u64,
    pub coalesced_64k_runs: u64,
    pub coalesced_64k_span_bytes: u64,
    pub sorted_contiguous_runs: u64,
    pub sorted_contiguous_span_bytes: u64,
    pub sorted_coalesced_4k_runs: u64,
    pub sorted_coalesced_4k_span_bytes: u64,
    pub sorted_coalesced_64k_runs: u64,
    pub sorted_coalesced_64k_span_bytes: u64,
    initialized: bool,
    last_offset: u64,
    last_end: u64,
    contiguous_end: u64,
    coalesced_4k_end: u64,
    coalesced_64k_end: u64,
}

impl ChunkPayloadLocalityProfile {
    const GAP_4K: u64 = 4 * 1024;
    const GAP_64K: u64 = 64 * 1024;

    fn observe(&mut self, offset: u64, len: u64) {
        let end = offset.saturating_add(len);
        let backward_jump = self.initialized && offset < self.last_offset;

        self.reads = self.reads.saturating_add(1);
        if self.initialized {
            if backward_jump {
                self.backward_jumps = self.backward_jumps.saturating_add(1);
            } else if offset > self.last_end {
                let gap = offset - self.last_end;
                self.forward_gaps = self.forward_gaps.saturating_add(1);
                self.forward_gap_bytes = self.forward_gap_bytes.saturating_add(gap);
            }
        }

        observe_coalesced_range(
            offset,
            end,
            0,
            backward_jump,
            &mut self.contiguous_runs,
            &mut self.contiguous_span_bytes,
            &mut self.contiguous_end,
        );
        observe_coalesced_range(
            offset,
            end,
            Self::GAP_4K,
            backward_jump,
            &mut self.coalesced_4k_runs,
            &mut self.coalesced_4k_span_bytes,
            &mut self.coalesced_4k_end,
        );
        observe_coalesced_range(
            offset,
            end,
            Self::GAP_64K,
            backward_jump,
            &mut self.coalesced_64k_runs,
            &mut self.coalesced_64k_span_bytes,
            &mut self.coalesced_64k_end,
        );

        self.initialized = true;
        self.last_offset = offset;
        self.last_end = end;
    }

    fn observe_sorted(&mut self, ranges: &mut [(u64, u64)]) {
        if ranges.is_empty() {
            return;
        }

        ranges.sort_unstable_by(|left, right| {
            left.0.cmp(&right.0).then_with(|| left.1.cmp(&right.1))
        });

        let (runs, span_bytes) = coalesced_summary(ranges, 0);
        self.sorted_contiguous_runs = self.sorted_contiguous_runs.saturating_add(runs);
        self.sorted_contiguous_span_bytes =
            self.sorted_contiguous_span_bytes.saturating_add(span_bytes);

        let (runs, span_bytes) = coalesced_summary(ranges, Self::GAP_4K);
        self.sorted_coalesced_4k_runs = self.sorted_coalesced_4k_runs.saturating_add(runs);
        self.sorted_coalesced_4k_span_bytes = self
            .sorted_coalesced_4k_span_bytes
            .saturating_add(span_bytes);

        let (runs, span_bytes) = coalesced_summary(ranges, Self::GAP_64K);
        self.sorted_coalesced_64k_runs = self.sorted_coalesced_64k_runs.saturating_add(runs);
        self.sorted_coalesced_64k_span_bytes = self
            .sorted_coalesced_64k_span_bytes
            .saturating_add(span_bytes);
    }

    pub fn add(&mut self, other: Self) {
        self.reads = self.reads.saturating_add(other.reads);
        self.forward_gaps = self.forward_gaps.saturating_add(other.forward_gaps);
        self.forward_gap_bytes = self
            .forward_gap_bytes
            .saturating_add(other.forward_gap_bytes);
        self.backward_jumps = self.backward_jumps.saturating_add(other.backward_jumps);
        self.contiguous_runs = self.contiguous_runs.saturating_add(other.contiguous_runs);
        self.contiguous_span_bytes = self
            .contiguous_span_bytes
            .saturating_add(other.contiguous_span_bytes);
        self.coalesced_4k_runs = self
            .coalesced_4k_runs
            .saturating_add(other.coalesced_4k_runs);
        self.coalesced_4k_span_bytes = self
            .coalesced_4k_span_bytes
            .saturating_add(other.coalesced_4k_span_bytes);
        self.coalesced_64k_runs = self
            .coalesced_64k_runs
            .saturating_add(other.coalesced_64k_runs);
        self.coalesced_64k_span_bytes = self
            .coalesced_64k_span_bytes
            .saturating_add(other.coalesced_64k_span_bytes);
        self.sorted_contiguous_runs = self
            .sorted_contiguous_runs
            .saturating_add(other.sorted_contiguous_runs);
        self.sorted_contiguous_span_bytes = self
            .sorted_contiguous_span_bytes
            .saturating_add(other.sorted_contiguous_span_bytes);
        self.sorted_coalesced_4k_runs = self
            .sorted_coalesced_4k_runs
            .saturating_add(other.sorted_coalesced_4k_runs);
        self.sorted_coalesced_4k_span_bytes = self
            .sorted_coalesced_4k_span_bytes
            .saturating_add(other.sorted_coalesced_4k_span_bytes);
        self.sorted_coalesced_64k_runs = self
            .sorted_coalesced_64k_runs
            .saturating_add(other.sorted_coalesced_64k_runs);
        self.sorted_coalesced_64k_span_bytes = self
            .sorted_coalesced_64k_span_bytes
            .saturating_add(other.sorted_coalesced_64k_span_bytes);
    }

    fn delta_since(self, before: Self) -> Self {
        Self {
            reads: self.reads.saturating_sub(before.reads),
            forward_gaps: self.forward_gaps.saturating_sub(before.forward_gaps),
            forward_gap_bytes: self
                .forward_gap_bytes
                .saturating_sub(before.forward_gap_bytes),
            backward_jumps: self.backward_jumps.saturating_sub(before.backward_jumps),
            contiguous_runs: self.contiguous_runs.saturating_sub(before.contiguous_runs),
            contiguous_span_bytes: self
                .contiguous_span_bytes
                .saturating_sub(before.contiguous_span_bytes),
            coalesced_4k_runs: self
                .coalesced_4k_runs
                .saturating_sub(before.coalesced_4k_runs),
            coalesced_4k_span_bytes: self
                .coalesced_4k_span_bytes
                .saturating_sub(before.coalesced_4k_span_bytes),
            coalesced_64k_runs: self
                .coalesced_64k_runs
                .saturating_sub(before.coalesced_64k_runs),
            coalesced_64k_span_bytes: self
                .coalesced_64k_span_bytes
                .saturating_sub(before.coalesced_64k_span_bytes),
            sorted_contiguous_runs: self
                .sorted_contiguous_runs
                .saturating_sub(before.sorted_contiguous_runs),
            sorted_contiguous_span_bytes: self
                .sorted_contiguous_span_bytes
                .saturating_sub(before.sorted_contiguous_span_bytes),
            sorted_coalesced_4k_runs: self
                .sorted_coalesced_4k_runs
                .saturating_sub(before.sorted_coalesced_4k_runs),
            sorted_coalesced_4k_span_bytes: self
                .sorted_coalesced_4k_span_bytes
                .saturating_sub(before.sorted_coalesced_4k_span_bytes),
            sorted_coalesced_64k_runs: self
                .sorted_coalesced_64k_runs
                .saturating_sub(before.sorted_coalesced_64k_runs),
            sorted_coalesced_64k_span_bytes: self
                .sorted_coalesced_64k_span_bytes
                .saturating_sub(before.sorted_coalesced_64k_span_bytes),
            ..Self::default()
        }
    }
}

fn coalesced_summary(ranges: &[(u64, u64)], max_gap: u64) -> (u64, u64) {
    let mut runs = 0;
    let mut span_bytes = 0;
    let mut run_end = 0;
    for &(offset, len) in ranges {
        let end = offset.saturating_add(len);
        observe_coalesced_range(
            offset,
            end,
            max_gap,
            false,
            &mut runs,
            &mut span_bytes,
            &mut run_end,
        );
    }
    (runs, span_bytes)
}

fn observe_coalesced_range(
    offset: u64,
    end: u64,
    max_gap: u64,
    force_new_run: bool,
    runs: &mut u64,
    span_bytes: &mut u64,
    run_end: &mut u64,
) {
    let starts_new_run = *runs == 0 || force_new_run || offset > run_end.saturating_add(max_gap);
    if starts_new_run {
        *runs = (*runs).saturating_add(1);
        *span_bytes = (*span_bytes).saturating_add(end.saturating_sub(offset));
        *run_end = end;
    } else if end > *run_end {
        *span_bytes = (*span_bytes).saturating_add(end - *run_end);
        *run_end = end;
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ChunkReadSchedulerProfile {
    pub executions: u64,
    pub pread_decisions: u64,
    pub io_uring_decisions: u64,
    pub logical_requests: u64,
    pub physical_spans: u64,
    pub backend_submissions: u64,
    pub sqes_submitted: u64,
    pub submission_depth_sum: u64,
    pub submission_depth_max: u64,
    pub submission_depth_1: u64,
    pub submission_depth_2_3: u64,
    pub submission_depth_4_7: u64,
    pub submission_depth_8_plus: u64,
    /// Total physical bytes executed by the scheduler. Results may remain
    /// retained until their bounded scheduler group is decoded.
    pub total_physical_bytes_executed: u64,
    /// Session high-water mark for bytes concurrently submitted to a backend:
    /// one span for pread, or up to the configured queue depth for io_uring. A
    /// delta containing new executions retains the session maximum because
    /// maxima cannot be subtracted exactly.
    pub peak_in_flight_bytes: u64,
}

impl ChunkReadSchedulerProfile {
    pub fn add(&mut self, other: Self) {
        self.executions = self.executions.saturating_add(other.executions);
        self.pread_decisions = self.pread_decisions.saturating_add(other.pread_decisions);
        self.io_uring_decisions = self
            .io_uring_decisions
            .saturating_add(other.io_uring_decisions);
        self.logical_requests = self.logical_requests.saturating_add(other.logical_requests);
        self.physical_spans = self.physical_spans.saturating_add(other.physical_spans);
        self.backend_submissions = self
            .backend_submissions
            .saturating_add(other.backend_submissions);
        self.sqes_submitted = self.sqes_submitted.saturating_add(other.sqes_submitted);
        self.submission_depth_sum = self
            .submission_depth_sum
            .saturating_add(other.submission_depth_sum);
        self.submission_depth_max = self.submission_depth_max.max(other.submission_depth_max);
        self.submission_depth_1 = self
            .submission_depth_1
            .saturating_add(other.submission_depth_1);
        self.submission_depth_2_3 = self
            .submission_depth_2_3
            .saturating_add(other.submission_depth_2_3);
        self.submission_depth_4_7 = self
            .submission_depth_4_7
            .saturating_add(other.submission_depth_4_7);
        self.submission_depth_8_plus = self
            .submission_depth_8_plus
            .saturating_add(other.submission_depth_8_plus);
        self.total_physical_bytes_executed = self
            .total_physical_bytes_executed
            .saturating_add(other.total_physical_bytes_executed);
        self.peak_in_flight_bytes = self.peak_in_flight_bytes.max(other.peak_in_flight_bytes);
    }

    fn delta_since(self, before: Self) -> Self {
        let has_new_executions = self.executions > before.executions;
        Self {
            executions: self.executions.saturating_sub(before.executions),
            pread_decisions: self.pread_decisions.saturating_sub(before.pread_decisions),
            io_uring_decisions: self
                .io_uring_decisions
                .saturating_sub(before.io_uring_decisions),
            logical_requests: self
                .logical_requests
                .saturating_sub(before.logical_requests),
            physical_spans: self.physical_spans.saturating_sub(before.physical_spans),
            backend_submissions: self
                .backend_submissions
                .saturating_sub(before.backend_submissions),
            sqes_submitted: self.sqes_submitted.saturating_sub(before.sqes_submitted),
            submission_depth_sum: self
                .submission_depth_sum
                .saturating_sub(before.submission_depth_sum),
            submission_depth_max: if has_new_executions {
                self.submission_depth_max
            } else {
                0
            },
            submission_depth_1: self
                .submission_depth_1
                .saturating_sub(before.submission_depth_1),
            submission_depth_2_3: self
                .submission_depth_2_3
                .saturating_sub(before.submission_depth_2_3),
            submission_depth_4_7: self
                .submission_depth_4_7
                .saturating_sub(before.submission_depth_4_7),
            submission_depth_8_plus: self
                .submission_depth_8_plus
                .saturating_sub(before.submission_depth_8_plus),
            total_physical_bytes_executed: self
                .total_physical_bytes_executed
                .saturating_sub(before.total_physical_bytes_executed),
            peak_in_flight_bytes: if has_new_executions {
                self.peak_in_flight_bytes
            } else {
                0
            },
        }
    }
}

/// Store-wide snapshot of currently retained symbol-reader resources.
///
/// One shared reader state may be cloned into multiple query sessions. The
/// collector deduplicates those states before filling this snapshot, so every
/// field is a current-value gauge rather than a per-session counter.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SegmentStoreSymbolResources {
    pub retained_readers: u64,
    pub retained_open_files: u64,
    pub source_file_bytes: u64,
    pub root_encoded_bytes: u64,
    pub root_retained_charge_bytes: u64,
    pub eager_dictionary_retained_charge_bytes: u64,
    pub page_cache_charge_bytes: u64,
    pub page_cache_max_bytes: u64,
    pub snapshot_errors: u64,
}

impl SegmentStoreSymbolResources {
    fn observe(&mut self, resources: SegmentSymbolResourceSnapshot) {
        self.retained_readers = self.retained_readers.saturating_add(1);
        self.retained_open_files = self
            .retained_open_files
            .saturating_add(resources.retained_open_files);
        self.source_file_bytes = self
            .source_file_bytes
            .saturating_add(resources.source_file_bytes);
        self.root_encoded_bytes = self
            .root_encoded_bytes
            .saturating_add(resources.root_encoded_bytes);
        self.root_retained_charge_bytes = self
            .root_retained_charge_bytes
            .saturating_add(resources.root_retained_charge_bytes);
        self.eager_dictionary_retained_charge_bytes = self
            .eager_dictionary_retained_charge_bytes
            .saturating_add(resources.eager_dictionary_retained_charge_bytes);
        self.page_cache_charge_bytes = self
            .page_cache_charge_bytes
            .saturating_add(resources.page_cache_charge_bytes);
        self.page_cache_max_bytes = self
            .page_cache_max_bytes
            .saturating_add(resources.page_cache_max_bytes);
    }

    pub fn total_retained_charge_bytes(self) -> u64 {
        self.root_retained_charge_bytes
            .saturating_add(self.eager_dictionary_retained_charge_bytes)
            .saturating_add(self.page_cache_charge_bytes)
    }

    pub(in crate::storage::segment) fn snapshot_segment_readers<'a>(
        readers: impl IntoIterator<Item = &'a SegmentReader>,
    ) -> Self {
        let mut snapshot = Self::default();
        let mut seen_states = BTreeSet::new();
        for reader in readers {
            let cached = match reader.query_cache.symbols.lock() {
                Ok(cached) => cached,
                Err(_) => {
                    snapshot.snapshot_errors = snapshot.snapshot_errors.saturating_add(1);
                    continue;
                }
            };
            let Some(symbols) = cached.as_ref() else {
                continue;
            };
            if !seen_states.insert(symbols.state_identity()) {
                continue;
            }
            match symbols.resource_snapshot() {
                Ok(resources) => snapshot.observe(resources),
                Err(_) => {
                    snapshot.snapshot_errors = snapshot.snapshot_errors.saturating_add(1);
                }
            }
        }
        snapshot
    }
}

/// Mutually exclusive elapsed-time attribution for query execution stages.
///
/// These fields are leaf-stage diagnostics. They may be summed with
/// [`Self::total_exclusive`]. The older open/read durations on
/// [`SegmentStoreQueryProfile`] are inclusive I/O diagnostics and must not be
/// added to this profile.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct QueryStageProfile {
    pub canonical_row_decode: Duration,
    pub symbol_lookup: Duration,
    pub symbol_resolution: Duration,
    /// Index/postings/FST traversal and set work used to produce candidate
    /// series references. This is separate from authoritative row matching.
    pub candidate_selection: Duration,
    pub canonical_identity: Duration,
    /// Schema-neutral visit, cache/governor, and callback-dispatch overhead
    /// left after the explicitly attributed canonical-row leaf work.
    pub metadata_visit_overhead: Duration,
    pub matcher_evaluation: Duration,
    pub label_construction: Duration,
    pub locator_planning: Duration,
    /// Combined payload read pipeline. The ordinary per-segment path includes
    /// coalescing-plan construction and governed file acquisition; the cross-
    /// segment path starts at scheduler execution after planning.
    pub payload_io: Duration,
    /// Combined payload decode, projection, and result-processing work.
    pub payload_decode: Duration,
    pub source_merge: Duration,
    pub promql_grouping_evaluation: Duration,
    pub result_construction: Duration,
}

impl QueryStageProfile {
    pub fn total_exclusive(self) -> Duration {
        Duration::ZERO
            .saturating_add(self.canonical_row_decode)
            .saturating_add(self.symbol_lookup)
            .saturating_add(self.symbol_resolution)
            .saturating_add(self.candidate_selection)
            .saturating_add(self.canonical_identity)
            .saturating_add(self.metadata_visit_overhead)
            .saturating_add(self.matcher_evaluation)
            .saturating_add(self.label_construction)
            .saturating_add(self.locator_planning)
            .saturating_add(self.payload_io)
            .saturating_add(self.payload_decode)
            .saturating_add(self.source_merge)
            .saturating_add(self.promql_grouping_evaluation)
            .saturating_add(self.result_construction)
    }

    pub(in crate::storage::segment) fn add(&mut self, other: Self) {
        self.canonical_row_decode = self
            .canonical_row_decode
            .saturating_add(other.canonical_row_decode);
        self.symbol_lookup = self.symbol_lookup.saturating_add(other.symbol_lookup);
        self.symbol_resolution = self
            .symbol_resolution
            .saturating_add(other.symbol_resolution);
        self.candidate_selection = self
            .candidate_selection
            .saturating_add(other.candidate_selection);
        self.canonical_identity = self
            .canonical_identity
            .saturating_add(other.canonical_identity);
        self.metadata_visit_overhead = self
            .metadata_visit_overhead
            .saturating_add(other.metadata_visit_overhead);
        self.matcher_evaluation = self
            .matcher_evaluation
            .saturating_add(other.matcher_evaluation);
        self.label_construction = self
            .label_construction
            .saturating_add(other.label_construction);
        self.locator_planning = self.locator_planning.saturating_add(other.locator_planning);
        self.payload_io = self.payload_io.saturating_add(other.payload_io);
        self.payload_decode = self.payload_decode.saturating_add(other.payload_decode);
        self.source_merge = self.source_merge.saturating_add(other.source_merge);
        self.promql_grouping_evaluation = self
            .promql_grouping_evaluation
            .saturating_add(other.promql_grouping_evaluation);
        self.result_construction = self
            .result_construction
            .saturating_add(other.result_construction);
    }

    pub fn delta_since(self, before: Self) -> Self {
        Self {
            canonical_row_decode: self
                .canonical_row_decode
                .saturating_sub(before.canonical_row_decode),
            symbol_lookup: self.symbol_lookup.saturating_sub(before.symbol_lookup),
            symbol_resolution: self
                .symbol_resolution
                .saturating_sub(before.symbol_resolution),
            candidate_selection: self
                .candidate_selection
                .saturating_sub(before.candidate_selection),
            canonical_identity: self
                .canonical_identity
                .saturating_sub(before.canonical_identity),
            metadata_visit_overhead: self
                .metadata_visit_overhead
                .saturating_sub(before.metadata_visit_overhead),
            matcher_evaluation: self
                .matcher_evaluation
                .saturating_sub(before.matcher_evaluation),
            label_construction: self
                .label_construction
                .saturating_sub(before.label_construction),
            locator_planning: self
                .locator_planning
                .saturating_sub(before.locator_planning),
            payload_io: self.payload_io.saturating_sub(before.payload_io),
            payload_decode: self.payload_decode.saturating_sub(before.payload_decode),
            source_merge: self.source_merge.saturating_sub(before.source_merge),
            promql_grouping_evaluation: self
                .promql_grouping_evaluation
                .saturating_sub(before.promql_grouping_evaluation),
            result_construction: self
                .result_construction
                .saturating_sub(before.result_construction),
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SegmentStoreQueryProfile {
    pub index_routing_open: Duration,
    pub segment_context_open: Duration,
    pub indexes_open: Duration,
    pub symbols_read: Duration,
    pub series_open: Duration,
    pub chunk_index_open: Duration,
    pub chunks_open: Duration,
    pub routing_index_read: Duration,
    pub exact_postings_read: Duration,
    pub metric_series_ranges_read: Duration,
    pub series_entry_read: Duration,
    pub chunk_index_range_read: Duration,
    pub chunk_read: Duration,
    pub index_routing_file_bytes: u64,
    pub indexes_file_bytes: u64,
    pub symbols_file_bytes: u64,
    pub series_file_bytes: u64,
    pub chunk_index_file_bytes: u64,
    pub chunks_file_bytes: u64,
    pub routing_index_bytes: u64,
    pub exact_postings_bytes: u64,
    pub metric_series_ranges_bytes: u64,
    pub series_entries_read: u64,
    pub series_entry_read_batches: u64,
    pub series_entry_bytes: u64,
    pub label_rows_integrity_checked: u64,
    pub label_pairs_integrity_checked: u64,
    pub label_rows_full_materialized: u64,
    pub label_rows_selectively_materialized: u64,
    pub label_pairs_materialized: u64,
    pub label_pairs_omitted: u64,
    pub label_content_bytes_materialized: u64,
    pub chunk_index_range_bytes: u64,
    pub chunk_payload_bytes: u64,
    pub chunk_payload_physical_reads: u64,
    pub chunk_payload_physical_bytes: u64,
    pub index_read_stats: SegmentIndexReadStats,
    pub symbol_read_stats: SegmentSymbolReadStats,
    pub symbol_resources: SegmentStoreSymbolResources,
    pub chunk_payload_locality: ChunkPayloadLocalityProfile,
    pub chunk_read_scheduler: ChunkReadSchedulerProfile,
    pub stages: QueryStageProfile,
}

impl SegmentStoreQueryProfile {
    pub(in crate::storage::segment) fn observe_label_materialization(
        &mut self,
        integrity_checked_label_count: usize,
        labels_complete: bool,
        labels: &[(String, String)],
    ) {
        let integrity_checked = u64::try_from(integrity_checked_label_count).unwrap_or(u64::MAX);
        let materialized = u64::try_from(labels.len()).unwrap_or(u64::MAX);
        self.label_rows_integrity_checked = self.label_rows_integrity_checked.saturating_add(1);
        self.label_pairs_integrity_checked = self
            .label_pairs_integrity_checked
            .saturating_add(integrity_checked);
        if labels_complete {
            self.label_rows_full_materialized = self.label_rows_full_materialized.saturating_add(1);
        } else {
            self.label_rows_selectively_materialized =
                self.label_rows_selectively_materialized.saturating_add(1);
        }
        self.label_pairs_materialized = self.label_pairs_materialized.saturating_add(materialized);
        self.label_pairs_omitted = self
            .label_pairs_omitted
            .saturating_add(integrity_checked.saturating_sub(materialized));
        let content_bytes = labels.iter().fold(0u64, |total, (name, value)| {
            total
                .saturating_add(u64::try_from(name.len()).unwrap_or(u64::MAX))
                .saturating_add(u64::try_from(value.len()).unwrap_or(u64::MAX))
        });
        self.label_content_bytes_materialized = self
            .label_content_bytes_materialized
            .saturating_add(content_bytes);
    }

    pub(in crate::storage::segment) fn observe_query_label_materialization(
        &mut self,
        integrity_checked_label_count: usize,
        labels_complete: bool,
        labels: &QueryLabels,
    ) {
        let integrity_checked = u64::try_from(integrity_checked_label_count).unwrap_or(u64::MAX);
        let materialized = u64::try_from(labels.len()).unwrap_or(u64::MAX);
        self.label_rows_integrity_checked = self.label_rows_integrity_checked.saturating_add(1);
        self.label_pairs_integrity_checked = self
            .label_pairs_integrity_checked
            .saturating_add(integrity_checked);
        if labels_complete {
            self.label_rows_full_materialized = self.label_rows_full_materialized.saturating_add(1);
        } else {
            self.label_rows_selectively_materialized =
                self.label_rows_selectively_materialized.saturating_add(1);
        }
        self.label_pairs_materialized = self.label_pairs_materialized.saturating_add(materialized);
        self.label_pairs_omitted = self
            .label_pairs_omitted
            .saturating_add(integrity_checked.saturating_sub(materialized));
        let content_bytes = labels.pairs().fold(0u64, |total, (name, value)| {
            total
                .saturating_add(u64::try_from(name.len()).unwrap_or(u64::MAX))
                .saturating_add(u64::try_from(value.len()).unwrap_or(u64::MAX))
        });
        self.label_content_bytes_materialized = self
            .label_content_bytes_materialized
            .saturating_add(content_bytes);
    }

    /// Observes one payload file as its own address space.
    ///
    /// Offsets from `chunks.bin` and `ooo_chunks.bin` are not comparable. A
    /// temporary per-file stream prevents equal or decreasing offsets in two
    /// files from being reported as one contiguous run or a backward jump.
    pub(in crate::storage::segment) fn observe_chunk_payload_file_reads(
        &mut self,
        ranges: &[(u64, u64)],
    ) {
        let mut locality = ChunkPayloadLocalityProfile::default();
        for &(offset, len) in ranges {
            self.chunk_payload_bytes = self.chunk_payload_bytes.saturating_add(len);
            locality.observe(offset, len);
        }
        self.chunk_payload_locality.add(locality);
    }

    pub(in crate::storage::segment) fn observe_chunk_payload_physical_reads(
        &mut self,
        reads: u64,
        bytes: u64,
    ) {
        self.chunk_payload_physical_reads = self.chunk_payload_physical_reads.saturating_add(reads);
        self.chunk_payload_physical_bytes = self.chunk_payload_physical_bytes.saturating_add(bytes);
    }

    pub(in crate::storage::segment) fn observe_sorted_chunk_payload_ranges(
        &mut self,
        ranges: &mut [(u64, u64)],
    ) {
        self.chunk_payload_locality.observe_sorted(ranges);
    }

    pub(in crate::storage::segment) fn add(&mut self, other: Self) {
        self.index_routing_open = self
            .index_routing_open
            .saturating_add(other.index_routing_open);
        self.segment_context_open = self
            .segment_context_open
            .saturating_add(other.segment_context_open);
        self.indexes_open = self.indexes_open.saturating_add(other.indexes_open);
        self.symbols_read = self.symbols_read.saturating_add(other.symbols_read);
        self.series_open = self.series_open.saturating_add(other.series_open);
        self.chunk_index_open = self.chunk_index_open.saturating_add(other.chunk_index_open);
        self.chunks_open = self.chunks_open.saturating_add(other.chunks_open);
        self.routing_index_read = self
            .routing_index_read
            .saturating_add(other.routing_index_read);
        self.exact_postings_read = self
            .exact_postings_read
            .saturating_add(other.exact_postings_read);
        self.metric_series_ranges_read = self
            .metric_series_ranges_read
            .saturating_add(other.metric_series_ranges_read);
        self.series_entry_read = self
            .series_entry_read
            .saturating_add(other.series_entry_read);
        self.chunk_index_range_read = self
            .chunk_index_range_read
            .saturating_add(other.chunk_index_range_read);
        self.chunk_read = self.chunk_read.saturating_add(other.chunk_read);
        self.index_routing_file_bytes = self
            .index_routing_file_bytes
            .saturating_add(other.index_routing_file_bytes);
        self.indexes_file_bytes = self
            .indexes_file_bytes
            .saturating_add(other.indexes_file_bytes);
        self.symbols_file_bytes = self
            .symbols_file_bytes
            .saturating_add(other.symbols_file_bytes);
        self.series_file_bytes = self
            .series_file_bytes
            .saturating_add(other.series_file_bytes);
        self.chunk_index_file_bytes = self
            .chunk_index_file_bytes
            .saturating_add(other.chunk_index_file_bytes);
        self.chunks_file_bytes = self
            .chunks_file_bytes
            .saturating_add(other.chunks_file_bytes);
        self.routing_index_bytes = self
            .routing_index_bytes
            .saturating_add(other.routing_index_bytes);
        self.exact_postings_bytes = self
            .exact_postings_bytes
            .saturating_add(other.exact_postings_bytes);
        self.metric_series_ranges_bytes = self
            .metric_series_ranges_bytes
            .saturating_add(other.metric_series_ranges_bytes);
        self.series_entries_read = self
            .series_entries_read
            .saturating_add(other.series_entries_read);
        self.series_entry_read_batches = self
            .series_entry_read_batches
            .saturating_add(other.series_entry_read_batches);
        self.series_entry_bytes = self
            .series_entry_bytes
            .saturating_add(other.series_entry_bytes);
        self.label_rows_integrity_checked = self
            .label_rows_integrity_checked
            .saturating_add(other.label_rows_integrity_checked);
        self.label_pairs_integrity_checked = self
            .label_pairs_integrity_checked
            .saturating_add(other.label_pairs_integrity_checked);
        self.label_rows_full_materialized = self
            .label_rows_full_materialized
            .saturating_add(other.label_rows_full_materialized);
        self.label_rows_selectively_materialized = self
            .label_rows_selectively_materialized
            .saturating_add(other.label_rows_selectively_materialized);
        self.label_pairs_materialized = self
            .label_pairs_materialized
            .saturating_add(other.label_pairs_materialized);
        self.label_pairs_omitted = self
            .label_pairs_omitted
            .saturating_add(other.label_pairs_omitted);
        self.label_content_bytes_materialized = self
            .label_content_bytes_materialized
            .saturating_add(other.label_content_bytes_materialized);
        self.chunk_index_range_bytes = self
            .chunk_index_range_bytes
            .saturating_add(other.chunk_index_range_bytes);
        self.chunk_payload_bytes = self
            .chunk_payload_bytes
            .saturating_add(other.chunk_payload_bytes);
        self.chunk_payload_physical_reads = self
            .chunk_payload_physical_reads
            .saturating_add(other.chunk_payload_physical_reads);
        self.chunk_payload_physical_bytes = self
            .chunk_payload_physical_bytes
            .saturating_add(other.chunk_payload_physical_bytes);
        self.index_read_stats = self.index_read_stats.saturating_add(other.index_read_stats);
        self.symbol_read_stats = self
            .symbol_read_stats
            .saturating_add(other.symbol_read_stats);
        self.symbol_resources = other.symbol_resources;
        self.chunk_payload_locality
            .add(other.chunk_payload_locality);
        self.chunk_read_scheduler.add(other.chunk_read_scheduler);
        self.stages.add(other.stages);
    }

    pub fn delta_since(self, before: Self) -> Self {
        Self {
            index_routing_open: self
                .index_routing_open
                .saturating_sub(before.index_routing_open),
            segment_context_open: self
                .segment_context_open
                .saturating_sub(before.segment_context_open),
            indexes_open: self.indexes_open.saturating_sub(before.indexes_open),
            symbols_read: self.symbols_read.saturating_sub(before.symbols_read),
            series_open: self.series_open.saturating_sub(before.series_open),
            chunk_index_open: self
                .chunk_index_open
                .saturating_sub(before.chunk_index_open),
            chunks_open: self.chunks_open.saturating_sub(before.chunks_open),
            routing_index_read: self
                .routing_index_read
                .saturating_sub(before.routing_index_read),
            exact_postings_read: self
                .exact_postings_read
                .saturating_sub(before.exact_postings_read),
            metric_series_ranges_read: self
                .metric_series_ranges_read
                .saturating_sub(before.metric_series_ranges_read),
            series_entry_read: self
                .series_entry_read
                .saturating_sub(before.series_entry_read),
            chunk_index_range_read: self
                .chunk_index_range_read
                .saturating_sub(before.chunk_index_range_read),
            chunk_read: self.chunk_read.saturating_sub(before.chunk_read),
            index_routing_file_bytes: self
                .index_routing_file_bytes
                .saturating_sub(before.index_routing_file_bytes),
            indexes_file_bytes: self
                .indexes_file_bytes
                .saturating_sub(before.indexes_file_bytes),
            symbols_file_bytes: self
                .symbols_file_bytes
                .saturating_sub(before.symbols_file_bytes),
            series_file_bytes: self
                .series_file_bytes
                .saturating_sub(before.series_file_bytes),
            chunk_index_file_bytes: self
                .chunk_index_file_bytes
                .saturating_sub(before.chunk_index_file_bytes),
            chunks_file_bytes: self
                .chunks_file_bytes
                .saturating_sub(before.chunks_file_bytes),
            routing_index_bytes: self
                .routing_index_bytes
                .saturating_sub(before.routing_index_bytes),
            exact_postings_bytes: self
                .exact_postings_bytes
                .saturating_sub(before.exact_postings_bytes),
            metric_series_ranges_bytes: self
                .metric_series_ranges_bytes
                .saturating_sub(before.metric_series_ranges_bytes),
            series_entries_read: self
                .series_entries_read
                .saturating_sub(before.series_entries_read),
            series_entry_read_batches: self
                .series_entry_read_batches
                .saturating_sub(before.series_entry_read_batches),
            series_entry_bytes: self
                .series_entry_bytes
                .saturating_sub(before.series_entry_bytes),
            label_rows_integrity_checked: self
                .label_rows_integrity_checked
                .saturating_sub(before.label_rows_integrity_checked),
            label_pairs_integrity_checked: self
                .label_pairs_integrity_checked
                .saturating_sub(before.label_pairs_integrity_checked),
            label_rows_full_materialized: self
                .label_rows_full_materialized
                .saturating_sub(before.label_rows_full_materialized),
            label_rows_selectively_materialized: self
                .label_rows_selectively_materialized
                .saturating_sub(before.label_rows_selectively_materialized),
            label_pairs_materialized: self
                .label_pairs_materialized
                .saturating_sub(before.label_pairs_materialized),
            label_pairs_omitted: self
                .label_pairs_omitted
                .saturating_sub(before.label_pairs_omitted),
            label_content_bytes_materialized: self
                .label_content_bytes_materialized
                .saturating_sub(before.label_content_bytes_materialized),
            chunk_index_range_bytes: self
                .chunk_index_range_bytes
                .saturating_sub(before.chunk_index_range_bytes),
            chunk_payload_bytes: self
                .chunk_payload_bytes
                .saturating_sub(before.chunk_payload_bytes),
            chunk_payload_physical_reads: self
                .chunk_payload_physical_reads
                .saturating_sub(before.chunk_payload_physical_reads),
            chunk_payload_physical_bytes: self
                .chunk_payload_physical_bytes
                .saturating_sub(before.chunk_payload_physical_bytes),
            index_read_stats: self
                .index_read_stats
                .saturating_sub(before.index_read_stats),
            symbol_read_stats: self.symbol_read_stats.delta_since(before.symbol_read_stats),
            // These are store-wide current-value gauges, not monotonic
            // counters. Preserve the after snapshot so warm-run deltas still
            // report all resources retained by the shared store.
            symbol_resources: self.symbol_resources,
            chunk_payload_locality: self
                .chunk_payload_locality
                .delta_since(before.chunk_payload_locality),
            chunk_read_scheduler: self
                .chunk_read_scheduler
                .delta_since(before.chunk_read_scheduler),
            stages: self.stages.delta_since(before.stages),
        }
    }
}

impl SegmentStoreQuerySessionStats {
    pub(in crate::storage::segment) fn add(&mut self, other: Self) {
        self.index_routing_opens = self
            .index_routing_opens
            .saturating_add(other.index_routing_opens);
        self.segment_context_opens = self
            .segment_context_opens
            .saturating_add(other.segment_context_opens);
        self.symbols_bin_opens = self
            .symbols_bin_opens
            .saturating_add(other.symbols_bin_opens);
        self.indexes_puffin_opens = self
            .indexes_puffin_opens
            .saturating_add(other.indexes_puffin_opens);
        self.series_bin_opens = self.series_bin_opens.saturating_add(other.series_bin_opens);
        self.chunk_index_bin_opens = self
            .chunk_index_bin_opens
            .saturating_add(other.chunk_index_bin_opens);
        self.chunks_bin_opens = self.chunks_bin_opens.saturating_add(other.chunks_bin_opens);
    }

    pub fn delta_since(self, before: Self) -> Self {
        Self {
            index_routing_opens: self
                .index_routing_opens
                .saturating_sub(before.index_routing_opens),
            segment_context_opens: self
                .segment_context_opens
                .saturating_sub(before.segment_context_opens),
            symbols_bin_opens: self
                .symbols_bin_opens
                .saturating_sub(before.symbols_bin_opens),
            indexes_puffin_opens: self
                .indexes_puffin_opens
                .saturating_sub(before.indexes_puffin_opens),
            series_bin_opens: self
                .series_bin_opens
                .saturating_sub(before.series_bin_opens),
            chunk_index_bin_opens: self
                .chunk_index_bin_opens
                .saturating_sub(before.chunk_index_bin_opens),
            chunks_bin_opens: self
                .chunks_bin_opens
                .saturating_sub(before.chunks_bin_opens),
        }
    }
}
