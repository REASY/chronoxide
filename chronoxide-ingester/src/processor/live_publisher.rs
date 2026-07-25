use super::*;

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use crate::error::ChronoxideError;
use chronoxide_core::storage::head::{
    FrozenFragmentIdentity, FrozenFragmentKey, FrozenHeadFragment, FrozenHeadLane,
    FrozenHeadReadView, HeadReadView, LivePartitionKey, LiveSampleStore, LiveSampleStoreBuilder,
    LiveSeriesCatalog, LiveSeriesCatalogBuilder,
};
use chronoxide_core::storage::live_memory::{
    LiveMemoryCharge, LiveMemoryClass, LiveMemoryGovernor,
};
use chronoxide_core::storage::live_view::{
    LiveCommitCandidate, LiveQueryHandle, LiveQueryView, LiveReadiness, LiveStorageView,
};
use chronoxide_core::storage::manifest::{
    ManifestCut, ManifestRecord, ManifestSegment, ManifestSnapshot, read_manifest_snapshot,
    refresh_manifest_snapshot,
};
use chronoxide_core::storage::segment::{
    SegmentFlushOutcome, SegmentId, SegmentStorageSchema, SegmentStoreReader, SegmentWriterConfig,
};

use super::live_seal::{build_frozen_segment_writer, finish_frozen_segment_writer};

/// Startup-only configuration for the embedded immutable live-query publisher.
#[derive(Debug, Clone)]
pub struct LivePublisherConfig {
    pub publish_interval: Duration,
    pub max_view_staleness: Duration,
    pub memory_admission_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct SealGroupKey {
    start_ms: u64,
    end_ms: u64,
    partition: PartitionKey,
    payload_lane: SegmentPayloadLane,
}

struct PendingSealAttempt {
    segment_id: SegmentId,
    fragment_identities: Vec<FrozenFragmentIdentity>,
    manifest_record: ManifestRecord,
    writer: Option<SegmentWriter>,
    committed_outcome: Option<SegmentFlushOutcome>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct HandoffRetirement {
    partition: PartitionKey,
    start_ms: u64,
    end_ms: u64,
    lane: FrozenHeadLane,
}

struct PendingFragment {
    partition: PartitionKey,
    fragment: Arc<FrozenHeadFragment>,
    estimated_bytes: u64,
    memory_charge: Option<Arc<LiveMemoryCharge>>,
    committed: bool,
    handed_off: bool,
}

/// Proof material for the shutdown-only empty-head construction.
///
/// This type is private so callers cannot request the shortcut directly. Its
/// constructor repeats the sealed-inventory binding check and also requires
/// exact coverage, pending ownership, no seal attempt, and empty mutable heads.
struct FinalEmptyHeadProof {
    committed_fragment_identities: Vec<FrozenFragmentIdentity>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PublicationFailureClass {
    RetryableResource,
    HealableOwnerConflict,
    TerminalIntegrity,
}

#[derive(Debug)]
struct OwnerValidationFailure {
    error: ChronoxideError,
    class: PublicationFailureClass,
}

#[derive(Debug, Clone, Copy, Default)]
struct OwnerValidationStats {
    active_partitions_capped: u8,
    run_keys_examined: u64,
    id_buckets: u64,
    canonical_identity_comparisons: u64,
    at_most_one_partition_fast_path: bool,
}

#[derive(Debug, Clone)]
struct RetainedPublicationFailure {
    stage: &'static str,
    message: Arc<str>,
}

impl RetainedPublicationFailure {
    fn as_error(&self) -> ChronoxideError {
        io::Error::other(format!(
            "earliest terminal live publication failure at {}: {}",
            self.stage, self.message
        ))
        .into()
    }
}

impl PendingFragment {
    fn fragment_key(&self) -> Result<FrozenFragmentKey> {
        FrozenFragmentKey::new(
            LivePartitionKey::new(
                Arc::<str>::from(self.partition.topic.as_str()),
                self.partition.partition,
            ),
            self.fragment.start_ms(),
            self.fragment.end_ms(),
            self.fragment.lane(),
        )
        .map_err(Into::into)
    }
}

/// Single-writer publication coordinator.
///
/// Mutable heads and this object remain owned by the ingestion thread. Query
/// workers receive only the immutable roots installed in `handle`.
pub(super) struct LivePublisher {
    config: LivePublisherConfig,
    segment_writer: SegmentWriterConfig,
    handle: Arc<LiveQueryHandle<LiveStorageView>>,
    memory: Arc<LiveMemoryGovernor>,
    manifest_snapshot: ManifestSnapshot,
    sealed: Arc<SegmentStoreReader>,
    sample_store: LiveSampleStore,
    catalog: Option<Arc<LiveSeriesCatalog>>,
    pending: Vec<PendingFragment>,
    seal_attempts: BTreeMap<SealGroupKey, PendingSealAttempt>,
    sealed_ranges: BTreeSet<(PartitionKey, u64, u64)>,
    sealed_coverage: CoverageLedger,
    /// Exact successful orders whose ownership is still represented by
    /// retained head fragments, including manifest-handed fragments until the
    /// new immutable root commits.
    expected_unsealed: RecordedSampleOrderSet,
    /// Worst-case exact runs reserved before datapoints can mutate the head in
    /// the active message.
    reserved_expected_runs: usize,
    last_published_at: Option<Instant>,
    latest_completed: Option<CompletedMessageCoverage>,
    /// A completed message remains owned here until its exact successful
    /// orders have been appended to `expected_unsealed`.
    unregistered_completion: Option<CompletedMessageCoverage>,
    latest_handoff_cut: Option<ManifestCut>,
    pending_handoff_records: Vec<ManifestRecord>,
    earliest_terminal_failure: Option<RetainedPublicationFailure>,
    owner_conflict_seen: bool,
    #[cfg(test)]
    publication_hook: Option<Arc<dyn Fn(PublicationStage) + Send + Sync>>,
    #[cfg(test)]
    next_head_decode_hook: Option<Arc<dyn Fn() + Send + Sync>>,
    #[cfg(test)]
    fail_next_expected_registration: bool,
    #[cfg(test)]
    fail_next_preflag_handoff_commit: bool,
    #[cfg(test)]
    overflow_next_handoff_coverage_merge: bool,
    #[cfg(test)]
    fail_next_commit_descriptor: bool,
}

#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PublicationStage {
    FrozenInputsRetained,
    ManifestHandoffCommitted,
    InventoryReady,
    CoverageValidated,
    SampleRetirementPathsReady,
    SampleDescriptorPathsReady,
    SampleRootReady,
    CatalogPagesReady,
    CatalogPostingsReady,
    CatalogReady,
    FinalEmptyRootsReady,
    OwnersValidated,
    HandoffRetirementReady,
    CommitDescriptorReady,
    RootSwapped,
}

impl LivePublisher {
    pub(super) fn new(
        config: LivePublisherConfig,
        segment_writer: SegmentWriterConfig,
    ) -> Result<Self> {
        if config.publish_interval.is_zero() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "live publication interval must be greater than zero",
            )
            .into());
        }
        if config.max_view_staleness < config.publish_interval {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "live maximum staleness must be at least the publication interval",
            )
            .into());
        }
        if segment_writer.storage_schema() != SegmentStorageSchema::Schema8 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "live publication requires a Schema 8 segment writer",
            )
            .into());
        }
        // Reuse the writer's authoritative-root/schema preflight at startup,
        // before a head-only root can be published. In particular, a missing
        // CURRENT may mean "new store" only when no top-level `seg-*` path
        // survives from an earlier publication.
        drop(SegmentWriter::new(segment_writer.clone())?);
        let memory = LiveMemoryGovernor::new(config.memory_admission_bytes)?;
        let handle = LiveQueryHandle::new(config.max_view_staleness).map_err(live_view_error)?;
        handle
            .configure_query_admission(Arc::clone(&memory), 1)
            .map_err(live_view_error)?;
        let manifest_snapshot =
            read_manifest_snapshot(segment_writer.segments_dir.join("manifest"))?;
        let sealed = Arc::new(SegmentStoreReader::open_manifest_snapshot(
            &segment_writer.segments_dir,
            &manifest_snapshot,
        )?);
        Ok(Self {
            config,
            segment_writer,
            handle,
            memory,
            manifest_snapshot,
            sealed,
            sample_store: LiveSampleStore::default(),
            catalog: None,
            pending: Vec::new(),
            seal_attempts: BTreeMap::new(),
            sealed_ranges: BTreeSet::new(),
            sealed_coverage: CoverageLedger::empty(),
            expected_unsealed: RecordedSampleOrderSet::empty(),
            reserved_expected_runs: 0,
            last_published_at: None,
            latest_completed: None,
            unregistered_completion: None,
            latest_handoff_cut: None,
            pending_handoff_records: Vec::new(),
            earliest_terminal_failure: None,
            owner_conflict_seen: false,
            #[cfg(test)]
            publication_hook: None,
            #[cfg(test)]
            next_head_decode_hook: None,
            #[cfg(test)]
            fail_next_expected_registration: false,
            #[cfg(test)]
            fail_next_preflag_handoff_commit: false,
            #[cfg(test)]
            overflow_next_handoff_coverage_merge: false,
            #[cfg(test)]
            fail_next_commit_descriptor: false,
        })
    }

    pub(super) fn handle(&self) -> Arc<LiveQueryHandle<LiveStorageView>> {
        Arc::clone(&self.handle)
    }

    pub(super) fn memory_governor(&self) -> Arc<LiveMemoryGovernor> {
        Arc::clone(&self.memory)
    }

    /// Reserves one worst-case exact ownership run before assigning a sample
    /// ordinal. A rejected datapoint merely leaves reusable spare capacity.
    pub(super) fn reserve_expected_order_slot(&mut self) -> Result<()> {
        if self.unregistered_completion.is_some() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "cannot reserve a new live order while completed ownership is unregistered",
            )
            .into());
        }
        let required_spare = self.reserved_expected_runs.checked_add(1).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "live expected-ownership reservation count overflows usize",
            )
        })?;
        self.expected_unsealed
            .try_reserve_additional_runs(required_spare)?;
        self.reserved_expected_runs = required_spare;
        Ok(())
    }

    /// Retries lossless registration of a prior completed message before a
    /// later acquired message can begin.
    pub(super) fn prepare_for_next_message(&mut self) -> Result<()> {
        if self.unregistered_completion.is_none() {
            return Ok(());
        }
        if let Err(error) = self.register_retained_completion() {
            let class = resource_or_integrity(&error);
            self.fail_readiness("message-cut", &error, class);
            return Err(error);
        }
        Ok(())
    }

    /// Starts staleness aging at the first committed mutable-head change,
    /// rather than granting a fresh deadline only after a potentially large
    /// OTLP message has finished.
    pub(super) fn on_head_mutation(&mut self, now: Instant) -> Result<()> {
        if let Err(error) = self.handle.mark_dirty(now) {
            let error = live_view_error(error);
            self.fail_readiness(
                "dirty-state",
                &error,
                PublicationFailureClass::TerminalIntegrity,
            );
            return Err(error);
        }
        Ok(())
    }

    pub(super) fn on_message_boundary(
        &mut self,
        sequence: MessageSequence,
        completed: CompletedMessageCoverage,
        heads: &mut HashMap<PartitionKey, PartitionHead>,
        labelsets: &mut LabelSetInterner,
    ) -> Result<()> {
        let boundary_started = Instant::now();
        if self.unregistered_completion.is_some() {
            let error = ChronoxideError::from(io::Error::new(
                io::ErrorKind::InvalidData,
                "publisher received a later completion before registering the retained one",
            ));
            self.fail_readiness(
                "message-cut",
                &error,
                PublicationFailureClass::TerminalIntegrity,
            );
            return Err(error);
        }
        self.unregistered_completion = Some(completed);
        if let Err(error) = self.register_retained_completion_for(sequence) {
            let class = resource_or_integrity(&error);
            self.fail_readiness("message-cut", &error, class);
            return Err(error);
        }
        if let Err(error) = self.handle.mark_dirty(Instant::now()) {
            let error = live_view_error(error);
            self.fail_readiness(
                "dirty-state",
                &error,
                PublicationFailureClass::TerminalIntegrity,
            );
            return Err(error);
        }

        let due_to_rotation = heads
            .values()
            .any(|partition| !partition.seal_ready_ranges.is_empty());
        let now = Instant::now();
        let status = match self.handle.status() {
            Ok(status) => status,
            Err(error) => {
                let error = live_view_error(error);
                self.fail_readiness(
                    "readiness-state",
                    &error,
                    PublicationFailureClass::TerminalIntegrity,
                );
                return Err(error);
            }
        };
        let due = self.last_published_at.is_none()
            || due_to_rotation
            || self.last_published_at.is_some_and(|last| {
                now.saturating_duration_since(last) >= self.config.publish_interval
            })
            || matches!(status.readiness, LiveReadiness::Failed(_));
        if !due {
            return Ok(());
        }
        let result = self.publish(heads, labelsets, false);
        tracing::debug!(
            target: "chronoxide_live_metrics",
            event = "message_boundary",
            message_sequence = sequence.get(),
            publication_due = true,
            outcome = if result.is_ok() { "success" } else { "failure" },
            ingestion_pause_ns = duration_ns(boundary_started.elapsed()),
            "Live message-boundary publication observation"
        );
        result
    }

    fn register_retained_completion(&mut self) -> Result<()> {
        let sequence = self
            .unregistered_completion
            .as_ref()
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "no retained live completion is available to register",
                )
            })?
            .message_sequence;
        self.register_retained_completion_for(sequence)
    }

    fn register_retained_completion_for(&mut self, sequence: MessageSequence) -> Result<()> {
        let completed = self.unregistered_completion.as_ref().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "publisher lost the completed message before exact registration",
            )
        })?;
        if completed.message_sequence != sequence {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "publisher received a completed ledger for a different message",
            )
            .into());
        }
        if self
            .latest_completed
            .as_ref()
            .is_some_and(|previous| previous.message_sequence >= sequence)
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "publisher message cut did not increase strictly",
            )
            .into());
        }
        completed.successful_orders.validate()?;
        if completed.successful_orders.sample_count() != completed.coverage.sample_count() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "completed live ledger disagrees with its exact successful orders",
            )
            .into());
        }
        if completed
            .successful_orders
            .runs()
            .iter()
            .any(|run| run.first().message_sequence() != sequence)
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "completed live exact orders belong to another message",
            )
            .into());
        }
        let prior_prefix = self
            .latest_completed
            .as_ref()
            .map_or(CoverageLedger::empty(), |previous| {
                previous.completed_prefix
            });
        let expected_prefix = prior_prefix.checked_merge(completed.coverage)?;
        if completed.completed_prefix != expected_prefix {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "completed live prefix does not extend the prior registered prefix",
            )
            .into());
        }

        #[cfg(test)]
        if std::mem::take(&mut self.fail_next_expected_registration) {
            return Err(io::Error::new(
                io::ErrorKind::OutOfMemory,
                "injected expected-unsealed registration failure",
            )
            .into());
        }

        // Capacity for the worst case was reserved before each datapoint could
        // mutate a head. This append is therefore allocation-free. On any
        // validation/capacity error both the prior expected set and the
        // retained completion remain unchanged.
        self.expected_unsealed
            .append_pre_reserved(&completed.successful_orders)?;
        self.latest_completed = self.unregistered_completion.take();
        self.reserved_expected_runs = 0;
        Ok(())
    }

    pub(super) fn shutdown(
        &mut self,
        heads: &mut HashMap<PartitionKey, PartitionHead>,
        labelsets: &mut LabelSetInterner,
    ) -> Result<()> {
        self.prepare_for_next_message()?;
        let final_publication = self.publish(heads, labelsets, true);
        if let Some(earliest) = &self.earliest_terminal_failure {
            if let Err(later) = &final_publication {
                error!(
                    earliest_stage = earliest.stage,
                    earliest_error = %earliest.message,
                    later_error = %later,
                    "Live shutdown retained the earliest terminal publication failure"
                );
            }
            return Err(earliest.as_error());
        }
        if final_publication.is_ok() && self.owner_conflict_seen {
            warn!("Final sealing healed an earlier cross-partition live-owner conflict");
        }
        final_publication
    }

    fn publish(
        &mut self,
        heads: &mut HashMap<PartitionKey, PartitionHead>,
        labelsets: &mut LabelSetInterner,
        seal_everything: bool,
    ) -> Result<()> {
        let publication_started = Instant::now();
        let base_sample_keys = self.sample_store.key_count();
        let base_sample_fragments = self.sample_store.fragment_count();
        let base_catalog_active_series = self
            .catalog
            .as_ref()
            .map_or(0, |catalog| catalog.active_series_len());
        let freeze_started = Instant::now();
        if let Err(error) = self.retry_missing_memory_charges() {
            self.fail_readiness(
                "memory-admission",
                &error,
                PublicationFailureClass::RetryableResource,
            );
            return Err(error);
        }
        if let Err(error) = self.freeze_heads(heads) {
            let class = resource_or_integrity(&error);
            self.fail_readiness("freeze", &error, class);
            return Err(error);
        }
        if let Err(error) = self.retry_missing_memory_charges() {
            self.fail_readiness(
                "memory-admission",
                &error,
                PublicationFailureClass::RetryableResource,
            );
            return Err(error);
        }
        #[cfg(test)]
        self.run_publication_hook(PublicationStage::FrozenInputsRetained);
        let freeze_and_admission_duration = freeze_started.elapsed();

        let seal_started = Instant::now();
        let seal_groups = match self.seal_groups(heads, seal_everything) {
            Ok(groups) => groups,
            Err(error) => {
                self.fail_readiness(
                    "seal-plan",
                    &error,
                    PublicationFailureClass::TerminalIntegrity,
                );
                return Err(error);
            }
        };
        for group in seal_groups {
            if let Err(error) = self.seal_group(&group, labelsets) {
                let class = resource_or_integrity(&error);
                self.fail_readiness("segment-seal", &error, class);
                return Err(error);
            }
        }
        if self.pending.iter().any(|pending| pending.handed_off) {
            #[cfg(test)]
            self.run_publication_hook(PublicationStage::ManifestHandoffCommitted);
        }
        let seal_duration = seal_started.elapsed();

        let inventory_started = Instant::now();
        let refreshed_inventory = if self.pending.iter().any(|pending| pending.handed_off) {
            match self.prepare_sealed_inventory_refresh() {
                Ok(refreshed) => Some(refreshed),
                Err(error) => {
                    self.fail_readiness(
                        "manifest-refresh",
                        &error,
                        PublicationFailureClass::TerminalIntegrity,
                    );
                    return Err(error);
                }
            }
        } else {
            None
        };
        let (candidate_manifest_cut, candidate_sealed) = match &refreshed_inventory {
            Some((snapshot, sealed)) => (snapshot.cut.clone(), Arc::clone(sealed)),
            None => (self.manifest_snapshot.cut.clone(), Arc::clone(&self.sealed)),
        };
        if candidate_sealed.validated_manifest_cut() != Some(&candidate_manifest_cut) {
            let error = ChronoxideError::from(io::Error::new(
                io::ErrorKind::InvalidData,
                "candidate manifest cut does not match the sealed inventory",
            ));
            self.fail_readiness(
                "manifest-binding",
                &error,
                PublicationFailureClass::TerminalIntegrity,
            );
            return Err(error);
        }
        #[cfg(test)]
        self.run_publication_hook(PublicationStage::InventoryReady);
        let inventory_duration = inventory_started.elapsed();

        let coverage_started = Instant::now();
        let empty_completed = CompletedMessageCoverage {
            message_sequence: MessageSequence::new(0),
            coverage: CoverageLedger::empty(),
            completed_prefix: CoverageLedger::empty(),
            successful_orders: RecordedSampleOrderSet::empty(),
        };
        let completed = self.latest_completed.as_ref().unwrap_or(&empty_completed);
        let visible_message_sequence = completed.message_sequence.get();
        let candidate_expected_unsealed = match self.validate_coverage(completed) {
            Ok(expected) => expected,
            Err(error) => {
                self.fail_readiness(
                    "coverage-validation",
                    &error,
                    PublicationFailureClass::TerminalIntegrity,
                );
                return Err(error);
            }
        };
        #[cfg(test)]
        self.run_publication_hook(PublicationStage::CoverageValidated);
        let coverage_duration = coverage_started.elapsed();

        let sample_root_started = Instant::now();
        let final_empty_head = match self.prepare_final_empty_head_proof(
            seal_everything,
            heads,
            &candidate_expected_unsealed,
            &candidate_manifest_cut,
            candidate_sealed.as_ref(),
            refreshed_inventory.is_some(),
        ) {
            Ok(proof) => proof,
            Err(error) => {
                let class = resource_or_integrity(&error);
                self.fail_readiness("final-empty-proof", &error, class);
                return Err(error);
            }
        };
        let final_empty_fast_path = final_empty_head.is_some();

        let candidate_store = match self.build_candidate_sample_store(final_empty_head.as_ref()) {
            Ok(store) => store,
            Err(error) => {
                let class = if final_empty_fast_path {
                    PublicationFailureClass::TerminalIntegrity
                } else {
                    resource_or_integrity(&error)
                };
                self.fail_readiness("sample-root", &error, class);
                return Err(error);
            }
        };
        #[cfg(test)]
        self.run_publication_hook(PublicationStage::SampleRootReady);
        let sample_root_duration = sample_root_started.elapsed();
        let catalog_started = Instant::now();
        let labels = match labelsets.live_snapshot() {
            Ok(labels) => Arc::new(labels),
            Err(error) => {
                let allocation_failed = matches!(
                    &error,
                    LabelSetStoreError::VersionedFlat(source)
                        if matches!(
                            source.as_ref(),
                            chronoxide_core::labels::VersionedFlatLabelStoreError::AllocationFailed {
                                ..
                            }
                        )
                );
                let error = ChronoxideError::from(io::Error::new(
                    if allocation_failed {
                        io::ErrorKind::OutOfMemory
                    } else {
                        io::ErrorKind::Other
                    },
                    error,
                ));
                let class = resource_or_integrity(&error);
                self.fail_readiness("catalog-snapshot", &error, class);
                return Err(error);
            }
        };
        #[cfg(test)]
        self.run_publication_hook(PublicationStage::CatalogPagesReady);
        let (base, begin_commit_timing) = match self.handle.begin_commit_timed() {
            Ok(prepared) => prepared,
            Err(error) => {
                let error = live_view_error(error);
                self.fail_readiness(
                    "commit-prepare",
                    &error,
                    PublicationFailureClass::TerminalIntegrity,
                );
                return Err(error);
            }
        };
        let catalog_builder_result = match (&self.catalog, final_empty_head.as_ref()) {
            (Some(previous), Some(_)) => {
                LiveSeriesCatalogBuilder::empty_successor(previous, labels, base.next_generation)
            }
            (Some(previous), None) => {
                LiveSeriesCatalogBuilder::from_catalog(previous, labels, base.next_generation)
            }
            (None, _) => LiveSeriesCatalogBuilder::new(labels, base.next_generation),
        }
        .map_err(ChronoxideError::from);
        let mut catalog_builder = match catalog_builder_result {
            Ok(builder) => builder,
            Err(error) => {
                let class = resource_or_integrity(&error);
                self.fail_readiness("catalog-root", &error, class);
                return Err(error);
            }
        };
        if final_empty_head.is_none() {
            if let Err(error) = catalog_builder.reconcile_sample_store(&candidate_store) {
                let error = ChronoxideError::from(error);
                let class = resource_or_integrity(&error);
                self.fail_readiness("catalog-root", &error, class);
                return Err(error);
            }
            #[cfg(test)]
            self.run_publication_hook(PublicationStage::CatalogPostingsReady);
        }
        let candidate_catalog = match catalog_builder.finish() {
            Ok(catalog) => Arc::new(catalog),
            Err(error) => {
                let error = ChronoxideError::from(error);
                let class = resource_or_integrity(&error);
                self.fail_readiness("catalog-root", &error, class);
                return Err(error);
            }
        };
        #[cfg(test)]
        self.run_publication_hook(PublicationStage::CatalogReady);
        #[cfg(test)]
        if final_empty_fast_path {
            self.run_publication_hook(PublicationStage::FinalEmptyRootsReady);
        }
        let catalog_duration = catalog_started.elapsed();
        let owner_and_head_started = Instant::now();
        let owner_validation_started = Instant::now();
        let owner_validation_stats =
            match self.validate_active_owners(&candidate_catalog, &candidate_store) {
                Ok(stats) => stats,
                Err(failure) => {
                    self.fail_readiness("owner-validation", &failure.error, failure.class);
                    return Err(failure.error);
                }
            };
        let owner_validation_duration = owner_validation_started.elapsed();
        #[cfg(test)]
        self.run_publication_hook(PublicationStage::OwnersValidated);
        let head_validation_started = Instant::now();
        let catalog_revision = candidate_catalog.revision();
        let frozen_head = FrozenHeadReadView::from_sample_store(candidate_store.clone());
        #[cfg(test)]
        let frozen_head = {
            let mut frozen_head = frozen_head;
            if let Some(hook) = self.next_head_decode_hook.take() {
                frozen_head.set_decode_hook_for_test(move || hook());
            }
            frozen_head
        };
        let head = match HeadReadView::new_live(
            Arc::new(frozen_head),
            Arc::clone(&candidate_catalog),
            base.next_generation,
        ) {
            Ok(head) => Arc::new(head),
            Err(error) => {
                let error = ChronoxideError::from(error);
                self.fail_readiness(
                    "head-view",
                    &error,
                    PublicationFailureClass::TerminalIntegrity,
                );
                return Err(error);
            }
        };
        if head.catalog_revision() != catalog_revision {
            let error = ChronoxideError::from(io::Error::new(
                io::ErrorKind::InvalidData,
                "candidate head catalog revision changed during single-writer construction",
            ));
            self.fail_readiness(
                "catalog-revision",
                &error,
                PublicationFailureClass::TerminalIntegrity,
            );
            return Err(error);
        }
        let head_validation_duration = head_validation_started.elapsed();
        let owner_and_head_duration = owner_and_head_started.elapsed();

        let root_build_started = Instant::now();
        let manifest_cut = candidate_manifest_cut;
        let manifest_validated_offset = manifest_cut.validated_offset();
        let manifest_present = matches!(&manifest_cut, ManifestCut::Present { .. });

        let retirements = match self.prepare_handed_off_retirements(heads) {
            Ok(retirements) => retirements,
            Err(error) => {
                self.fail_readiness(
                    "handoff-retirement",
                    &error,
                    PublicationFailureClass::TerminalIntegrity,
                );
                return Err(error);
            }
        };
        #[cfg(test)]
        self.run_publication_hook(PublicationStage::HandoffRetirementReady);

        let resource_leases = self
            .pending
            .iter()
            .filter(|pending| !pending.handed_off)
            .filter_map(|pending| pending.memory_charge.as_ref().map(Arc::clone))
            .collect();
        let payload =
            match LiveStorageView::with_resource_leases(candidate_sealed, head, resource_leases) {
                Ok(payload) => payload,
                Err(error) => {
                    let error = live_view_error(error);
                    self.fail_readiness(
                        "storage-root",
                        &error,
                        PublicationFailureClass::TerminalIntegrity,
                    );
                    return Err(error);
                }
            };
        #[cfg(test)]
        if std::mem::take(&mut self.fail_next_commit_descriptor) {
            let error = ChronoxideError::from(io::Error::new(
                io::ErrorKind::OutOfMemory,
                "injected live commit-descriptor preparation failure",
            ));
            self.fail_readiness(
                "commit-descriptor",
                &error,
                PublicationFailureClass::RetryableResource,
            );
            return Err(error);
        }
        // `commit_timed` finalizes the public view-age anchor immediately
        // before the atomic swap. The constructor retains this provisional
        // instant only for safe inspection of an unpublished candidate.
        let candidate_prepared_at = Instant::now();
        let view = match LiveQueryView::new_storage(
            base.next_generation,
            candidate_prepared_at,
            manifest_cut,
            visible_message_sequence,
            catalog_revision,
            payload,
        ) {
            Ok(view) => Arc::new(view),
            Err(error) => {
                let error = live_view_error(error);
                self.fail_readiness(
                    "view-root",
                    &error,
                    PublicationFailureClass::TerminalIntegrity,
                );
                return Err(error);
            }
        };
        let candidate = LiveCommitCandidate::new(base, view);
        #[cfg(test)]
        self.run_publication_hook(PublicationStage::CommitDescriptorReady);
        let root_build_duration = root_build_started.elapsed();
        let commit_timing = match self.handle.commit_timed(candidate) {
            Ok(timing) => timing,
            Err(error) => {
                let error = live_view_error(error);
                self.fail_readiness(
                    "root-commit",
                    &error,
                    PublicationFailureClass::TerminalIntegrity,
                );
                return Err(error);
            }
        };
        #[cfg(test)]
        self.run_publication_hook(PublicationStage::RootSwapped);

        let post_commit_started = Instant::now();
        if let Some((snapshot, sealed)) = refreshed_inventory {
            self.manifest_snapshot = snapshot;
            self.sealed = sealed;
            self.latest_handoff_cut = None;
            self.pending_handoff_records.clear();
        }
        self.sample_store = candidate_store;
        self.catalog = Some(candidate_catalog);
        // Only the immutable-root commit transfers manifest-handed ownership.
        // Until this point `expected_unsealed` still includes those exact
        // orders so every failed attempt remains retryable without weakening
        // the structural proof.
        self.expected_unsealed = candidate_expected_unsealed;
        for pending in &mut self.pending {
            if !pending.handed_off {
                pending.committed = true;
            }
        }
        self.apply_handed_off_retirements(heads, &retirements);
        // Coalescing is scheduled from completed publication, not from the
        // pre-swap age stamp stored in the immutable view.
        self.last_published_at = Some(Instant::now());
        let post_commit_duration = post_commit_started.elapsed();
        let publication_duration = publication_started.elapsed();
        if tracing::enabled!(
            target: "chronoxide_live_metrics",
            tracing::Level::DEBUG
        ) {
            let pending_fragment_count = self
                .pending
                .iter()
                .filter(|pending| !pending.handed_off)
                .count();
            let pending_estimated_bytes = self
                .pending
                .iter()
                .filter(|pending| !pending.handed_off)
                .fold(0_u64, |total, pending| {
                    total.saturating_add(pending.estimated_bytes)
                });
            let pending_arena_used_bytes = self
                .pending
                .iter()
                .filter(|pending| !pending.handed_off)
                .fold(0_u64, |total, pending| {
                    total.saturating_add(
                        u64::try_from(pending.fragment.arena_used_bytes()).unwrap_or(u64::MAX),
                    )
                });
            let pending_arena_allocated_bytes = self
                .pending
                .iter()
                .filter(|pending| !pending.handed_off)
                .fold(0_u64, |total, pending| {
                    total.saturating_add(
                        u64::try_from(pending.fragment.arena_allocated_bytes()).unwrap_or(u64::MAX),
                    )
                });
            let memory = self.memory.stats();
            let catalog = self
                .catalog
                .as_ref()
                .expect("successful publication installs a catalog");
            let catalog_memory = catalog.memory_estimate();
            tracing::debug!(
                target: "chronoxide_live_metrics",
                event = "publication",
                outcome = "success",
                mode = if seal_everything { "shutdown" } else { "boundary" },
                final_empty_fast_path,
                generation = base.next_generation,
                visible_message_sequence,
                catalog_revision,
                manifest_present,
                manifest_validated_offset,
                publication_duration_ns = duration_ns(publication_duration),
                freeze_and_admission_ns = duration_ns(freeze_and_admission_duration),
                seal_ns = duration_ns(seal_duration),
                inventory_ns = duration_ns(inventory_duration),
                coverage_ns = duration_ns(coverage_duration),
                sample_root_ns = duration_ns(sample_root_duration),
                catalog_ns = duration_ns(catalog_duration),
                owner_and_head_ns = duration_ns(owner_and_head_duration),
                owner_validation_ns = duration_ns(owner_validation_duration),
                head_validation_ns = duration_ns(head_validation_duration),
                owner_active_partitions_capped =
                    owner_validation_stats.active_partitions_capped,
                owner_run_keys_examined = owner_validation_stats.run_keys_examined,
                owner_id_buckets = owner_validation_stats.id_buckets,
                owner_canonical_identity_comparisons =
                    owner_validation_stats.canonical_identity_comparisons,
                owner_at_most_one_partition_fast_path =
                    owner_validation_stats.at_most_one_partition_fast_path,
                root_build_ns = duration_ns(root_build_duration),
                begin_commit_root_lock_wait_ns = duration_ns(begin_commit_timing.wait),
                begin_commit_root_lock_held_ns = duration_ns(begin_commit_timing.held),
                commit_root_lock_wait_ns = duration_ns(commit_timing.root_lock.wait),
                commit_root_lock_held_ns = duration_ns(commit_timing.root_lock.held),
                old_root_arc_drop_ns = duration_ns(commit_timing.old_root_reclaim),
                post_commit_ns = duration_ns(post_commit_duration),
                base_sample_keys,
                base_sample_fragments,
                base_catalog_active_series,
                pending_fragment_count,
                pending_estimated_bytes,
                pending_arena_used_bytes,
                pending_arena_allocated_bytes,
                sample_keys = self.sample_store.key_count(),
                sample_fragments = self.sample_store.fragment_count(),
                catalog_active_series = catalog.active_series_len(),
                catalog_shared_label_snapshot_bytes =
                    catalog_memory.shared_label_snapshot_bytes,
                catalog_index_bytes_if_unshared =
                    catalog_memory.catalog_index_bytes_if_unshared,
                live_memory_limit_bytes = memory.limit_bytes,
                live_memory_charged_bytes = memory.charged_bytes,
                live_memory_peak_charged_bytes = memory.peak_charged_bytes,
                live_mutable_tail_used_bytes = memory.mutable_tail_used_bytes,
                live_mutable_tail_capacity_bytes = memory.mutable_tail_capacity_bytes,
                live_memory_by_class = ?memory.by_class,
                "Live view publication observation"
            );
        }
        Ok(())
    }

    fn freeze_heads(&mut self, heads: &mut HashMap<PartitionKey, PartitionHead>) -> Result<()> {
        let mut partitions = heads.keys().cloned().collect::<Vec<_>>();
        partitions.sort_unstable();
        for partition in partitions {
            let head = &mut heads
                .get_mut(&partition)
                .expect("partition came from this head map")
                .head;
            // Reserve durable publisher ownership before the destructive
            // freeze. Every non-empty window is then moved into `pending`
            // before any memory-admission attempt can fail.
            self.pending
                .try_reserve(head.publication_fragment_count())
                .map_err(|error| {
                    io::Error::new(
                        io::ErrorKind::OutOfMemory,
                        format!("failed to reserve live pending fragments: {error}"),
                    )
                })?;
            match head.try_freeze_for_publication() {
                Ok(fragments) => {
                    self.retain_frozen_fragments(partition.clone(), fragments)?;
                }
                Err(error) => {
                    let (source, completed) = error.into_parts();
                    self.retain_frozen_fragments(partition.clone(), completed)?;
                    return Err(source.into());
                }
            }
        }
        Ok(())
    }

    fn retain_frozen_fragments(
        &mut self,
        partition: PartitionKey,
        fragments: Vec<FrozenHeadFragment>,
    ) -> Result<()> {
        let mut untracked = false;
        for fragment in fragments {
            if fragment.is_empty() {
                continue;
            }
            untracked |= !fragment.coverage_tracking_enabled();
            let estimated = fragment
                .arena_allocated_bytes()
                .saturating_add(fragment.estimated_run_bytes());
            let estimated = u64::try_from(estimated).unwrap_or(u64::MAX);
            debug_assert!(
                self.pending.len() < self.pending.capacity(),
                "freeze capacity was reserved before removing mutable windows"
            );
            self.pending.push(PendingFragment {
                partition: partition.clone(),
                fragment: Arc::new(fragment),
                estimated_bytes: estimated,
                memory_charge: None,
                committed: false,
                handed_off: false,
            });
        }
        if untracked {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "live publisher received an untracked frozen fragment",
            )
            .into());
        }
        Ok(())
    }

    fn retry_missing_memory_charges(&mut self) -> Result<()> {
        for pending in self
            .pending
            .iter_mut()
            .filter(|pending| pending.memory_charge.is_none())
        {
            let charge = self
                .memory
                .try_charge(LiveMemoryClass::FrozenPayload, pending.estimated_bytes)?;
            pending.memory_charge = Some(Arc::new(charge));
        }
        Ok(())
    }

    fn seal_groups(
        &self,
        heads: &HashMap<PartitionKey, PartitionHead>,
        seal_everything: bool,
    ) -> Result<Vec<SealGroupKey>> {
        let mut groups = BTreeSet::new();
        if seal_everything {
            let mut ranges = BTreeSet::new();
            for pending in self.pending.iter().filter(|pending| !pending.handed_off) {
                ranges.insert((
                    pending.partition.clone(),
                    pending.fragment.start_ms(),
                    pending.fragment.end_ms(),
                ));
            }
            for (partition, start_ms, end_ms) in ranges {
                let has_in_order = self.pending.iter().any(|pending| {
                    !pending.handed_off
                        && pending.partition == partition
                        && pending.fragment.start_ms() == start_ms
                        && pending.fragment.end_ms() == end_ms
                        && pending.fragment.lane() == FrozenHeadLane::InOrder
                });
                groups.insert(SealGroupKey {
                    partition,
                    start_ms,
                    end_ms,
                    payload_lane: if has_in_order {
                        SegmentPayloadLane::InOrder
                    } else {
                        SegmentPayloadLane::OutOfOrder
                    },
                });
            }
        } else {
            for (partition, state) in heads {
                for &(start_ms, end_ms) in &state.seal_ready_ranges {
                    if !self.pending.iter().any(|pending| {
                        !pending.handed_off
                            && pending.partition == *partition
                            && pending.fragment.start_ms() == start_ms
                            && pending.fragment.end_ms() == end_ms
                            && pending.fragment.lane() == FrozenHeadLane::InOrder
                    }) {
                        continue;
                    }
                    groups.insert(SealGroupKey {
                        partition: partition.clone(),
                        start_ms,
                        end_ms,
                        payload_lane: SegmentPayloadLane::InOrder,
                    });
                }
            }
            for pending in self.pending.iter().filter(|pending| {
                !pending.handed_off && pending.fragment.lane() == FrozenHeadLane::OutOfOrder
            }) {
                if self.sealed_ranges.contains(&(
                    pending.partition.clone(),
                    pending.fragment.start_ms(),
                    pending.fragment.end_ms(),
                )) {
                    groups.insert(SealGroupKey {
                        partition: pending.partition.clone(),
                        start_ms: pending.fragment.start_ms(),
                        end_ms: pending.fragment.end_ms(),
                        payload_lane: SegmentPayloadLane::OutOfOrder,
                    });
                }
            }
        }

        for group in &groups {
            let fragments = self.group_fragments(group);
            if fragments.is_empty() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "live seal plan references no pending fragments",
                )
                .into());
            }
            if group.payload_lane == SegmentPayloadLane::InOrder
                && !fragments
                    .iter()
                    .any(|fragment| fragment.lane() == FrozenHeadLane::InOrder)
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "normal live seal plan has no in-order fragment",
                )
                .into());
            }
        }
        Ok(groups.into_iter().collect())
    }

    fn group_fragments(&self, group: &SealGroupKey) -> Vec<Arc<FrozenHeadFragment>> {
        self.pending
            .iter()
            .filter(|pending| {
                !pending.handed_off
                    && pending.partition == group.partition
                    && pending.fragment.start_ms() == group.start_ms
                    && pending.fragment.end_ms() == group.end_ms
                    && (group.payload_lane == SegmentPayloadLane::InOrder
                        || pending.fragment.lane() == FrozenHeadLane::OutOfOrder)
            })
            .map(|pending| Arc::clone(&pending.fragment))
            .collect()
    }

    fn seal_group(&mut self, group: &SealGroupKey, labelsets: &LabelSetInterner) -> Result<()> {
        if !self.seal_attempts.contains_key(group) {
            let fragments = self.group_fragments(group);
            if fragments.is_empty() {
                return Ok(());
            }
            let fragment_identities = fragments
                .iter()
                .map(|fragment| self.fragment_identity(group, fragment))
                .collect::<Result<Vec<_>>>()?;
            let segment_id = self
                .segment_writer
                .allocate_segment_id(group.start_ms, group.end_ms)?;
            let manifest_record = ManifestRecord::SegmentSealed(ManifestSegment::new(
                segment_id.dir_name(),
                group.start_ms,
                group.end_ms,
                None,
            )?);
            self.seal_attempts.insert(
                group.clone(),
                PendingSealAttempt {
                    segment_id,
                    fragment_identities,
                    manifest_record,
                    writer: None,
                    committed_outcome: None,
                },
            );
        }

        // A retry is bound to the exact immutable input set captured when the
        // attempt was created. Later same-range fragments remain head-owned
        // and must never be retired by reconciliation of an older segment.
        let attempt_identities = self
            .seal_attempts
            .get(group)
            .expect("seal attempt was inserted immediately above")
            .fragment_identities
            .clone();
        let fragments = self.fragments_for_attempt(&attempt_identities)?;
        let retained_outcome = self
            .seal_attempts
            .get(group)
            .and_then(|attempt| attempt.committed_outcome.clone());
        let outcome = if let Some(outcome) = retained_outcome {
            outcome
        } else {
            self.pending_handoff_records
                .try_reserve(1)
                .map_err(|error| io::Error::new(io::ErrorKind::OutOfMemory, error))?;
            let attempt = self
                .seal_attempts
                .get_mut(group)
                .expect("seal attempt was inserted immediately above");
            if attempt.writer.is_none() {
                attempt.writer = Some(build_frozen_segment_writer(
                    self.segment_writer.clone(),
                    attempt.segment_id,
                    labelsets,
                    group.start_ms,
                    group.end_ms,
                    group.payload_lane,
                    &fragments,
                )?);
            }
            let finish_result = finish_frozen_segment_writer(
                attempt
                    .writer
                    .as_mut()
                    .expect("seal writer was built immediately above"),
            );
            let outcome = match finish_result {
                Err(error) => {
                    let retryable_manifest = attempt
                        .writer
                        .as_ref()
                        .is_some_and(SegmentWriter::has_retryable_manifest_attempt);
                    if !retryable_manifest {
                        attempt.writer = None;
                    }
                    return Err(error);
                }
                Ok(Some(outcome)) => outcome,
                Ok(None) => {
                    // A failed pre-manifest seal consumes the writer's active
                    // segment. Rebuild it from the exact retained attempt
                    // members on the next boundary while preserving the ID.
                    attempt.writer = None;
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "live seal attempt had no retryable manifest outcome; rebuilding is required",
                    )
                    .into());
                }
            };
            self.validate_flush_outcome(group, &outcome)?;
            let manifest_record = self
                .seal_attempts
                .get(group)
                .expect("validated flush belongs to the retained attempt")
                .manifest_record
                .clone();
            self.pending_handoff_records.push(manifest_record);
            self.seal_attempts
                .get_mut(group)
                .expect("validated flush belongs to the retained attempt")
                .committed_outcome = Some(outcome.clone());
            outcome
        };

        let (handoff_indices, handoff_coverage) =
            self.prepare_attempt_handoff(&attempt_identities)?;
        let sealed_coverage = self.sealed_coverage.checked_merge(handoff_coverage)?;
        #[cfg(test)]
        if std::mem::take(&mut self.fail_next_preflag_handoff_commit) {
            return Err(io::Error::other("injected pre-flag live handoff commit failure").into());
        }
        for index in handoff_indices {
            let pending = &mut self.pending[index];
            pending.handed_off = true;
        }
        self.sealed_coverage = sealed_coverage;
        self.latest_handoff_cut = Some(outcome.manifest_cut.clone());
        if group.payload_lane == SegmentPayloadLane::InOrder {
            self.sealed_ranges
                .insert((group.partition.clone(), group.start_ms, group.end_ms));
        }
        self.seal_attempts.remove(group);
        Ok(())
    }

    /// Resolves one retained seal attempt to exactly its original pending
    /// fragments without admitting later fragments for the same range/lane.
    ///
    /// Coverage is completed before any ownership flag changes, so allocation,
    /// identity, or ledger failure leaves the whole attempt retryable.
    fn prepare_attempt_handoff(
        &mut self,
        attempt_identities: &[FrozenFragmentIdentity],
    ) -> Result<(Vec<usize>, CoverageLedger)> {
        #[cfg(test)]
        let inject_checked_merge_overflow =
            std::mem::take(&mut self.overflow_next_handoff_coverage_merge);
        let mut handoff_coverage = CoverageLedger::empty();
        let mut handoff_indices = Vec::new();
        handoff_indices
            .try_reserve_exact(attempt_identities.len())
            .map_err(|error| io::Error::new(io::ErrorKind::OutOfMemory, error))?;
        for (index, pending) in self.pending.iter().enumerate() {
            if pending.handed_off {
                continue;
            }
            let identity = self.pending_fragment_identity(pending)?;
            if !attempt_identities.contains(&identity) {
                continue;
            }
            #[cfg(test)]
            if inject_checked_merge_overflow {
                // Start from a real retained fragment ledger and repeatedly
                // use the production checked merge. A non-empty fragment must
                // overflow within `u64::BITS` doublings. This exercises the
                // exact error path without constructing an invalid ledger or
                // mutating publisher ownership state.
                let mut overflowing = pending.fragment.coverage();
                for _ in 0..u64::BITS {
                    overflowing = overflowing.checked_merge(overflowing)?;
                }
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "injected handoff ledger unexpectedly remained empty",
                )
                .into());
            }
            handoff_coverage = handoff_coverage.checked_merge(pending.fragment.coverage())?;
            handoff_indices.push(index);
        }
        if handoff_indices.len() != attempt_identities.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "live seal attempt lost or duplicated one of its exact frozen inputs",
            )
            .into());
        }
        Ok((handoff_indices, handoff_coverage))
    }

    fn fragment_identity(
        &self,
        group: &SealGroupKey,
        fragment: &FrozenHeadFragment,
    ) -> Result<FrozenFragmentIdentity> {
        FrozenFragmentIdentity::for_fragment(
            LivePartitionKey::new(
                Arc::<str>::from(group.partition.topic.as_str()),
                group.partition.partition,
            ),
            fragment,
        )
        .map_err(Into::into)
    }

    fn pending_fragment_identity(
        &self,
        pending: &PendingFragment,
    ) -> Result<FrozenFragmentIdentity> {
        FrozenFragmentIdentity::for_fragment(
            LivePartitionKey::new(
                Arc::<str>::from(pending.partition.topic.as_str()),
                pending.partition.partition,
            ),
            pending.fragment.as_ref(),
        )
        .map_err(Into::into)
    }

    fn fragments_for_attempt(
        &self,
        identities: &[FrozenFragmentIdentity],
    ) -> Result<Vec<Arc<FrozenHeadFragment>>> {
        let mut fragments = Vec::new();
        fragments
            .try_reserve_exact(identities.len())
            .map_err(|error| io::Error::new(io::ErrorKind::OutOfMemory, error))?;
        for identity in identities {
            let mut matched = None;
            for pending in self.pending.iter().filter(|pending| !pending.handed_off) {
                if self.pending_fragment_identity(pending)? != *identity {
                    continue;
                }
                if matched.is_some() {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "live seal attempt identity matches duplicate pending fragments",
                    )
                    .into());
                }
                matched = Some(Arc::clone(&pending.fragment));
            }
            fragments.push(matched.ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "live seal attempt lost one of its retained frozen inputs",
                )
            })?);
        }
        Ok(fragments)
    }

    fn validate_flush_outcome(
        &self,
        group: &SealGroupKey,
        outcome: &SegmentFlushOutcome,
    ) -> Result<()> {
        if outcome.meta.start_ms != group.start_ms
            || outcome.meta.end_ms != group.end_ms
            || outcome.meta.segment_id
                != self
                    .seal_attempts
                    .get(group)
                    .expect("flush outcome belongs to an active attempt")
                    .segment_id
                    .dir_name()
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "live seal outcome differs from its retained logical attempt",
            )
            .into());
        }
        Ok(())
    }

    fn prepare_sealed_inventory_refresh(
        &self,
    ) -> Result<(ManifestSnapshot, Arc<SegmentStoreReader>)> {
        let next = refresh_manifest_snapshot(
            self.segment_writer.segments_dir.join("manifest"),
            &self.manifest_snapshot,
        )?;
        let sealed = self
            .sealed
            .refresh_manifest_snapshot(&self.segment_writer.segments_dir, &next)?;
        if self.latest_handoff_cut.as_ref() != Some(&next.cut) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "refreshed manifest cut differs from the latest live handoff",
            )
            .into());
        }
        let suffix = next
            .records
            .get(self.manifest_snapshot.records.len()..)
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "refreshed manifest record stream is shorter than the committed live cut",
                )
            })?;
        if suffix != self.pending_handoff_records.as_slice() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "refreshed manifest suffix differs from the exact ordered live handoff records",
            )
            .into());
        }
        Ok((next, Arc::new(sealed)))
    }

    fn validate_active_owners(
        &self,
        catalog: &LiveSeriesCatalog,
        candidate_store: &LiveSampleStore,
    ) -> std::result::Result<OwnerValidationStats, OwnerValidationFailure> {
        let mut stats = OwnerValidationStats::default();
        let active_fragment_count = self
            .pending
            .iter()
            .filter(|pending| !pending.handed_off)
            .count();
        let mut active_fragment_identities = Vec::new();
        active_fragment_identities
            .try_reserve_exact(active_fragment_count)
            .map_err(|error| OwnerValidationFailure {
                error: io::Error::new(io::ErrorKind::OutOfMemory, error).into(),
                class: PublicationFailureClass::RetryableResource,
            })?;
        for pending in self.pending.iter().filter(|pending| !pending.handed_off) {
            let identity = self.pending_fragment_identity(pending).map_err(|error| {
                OwnerValidationFailure {
                    class: resource_or_integrity(&error),
                    error,
                }
            })?;
            active_fragment_identities.push(identity);
        }
        active_fragment_identities.sort_unstable();
        if active_fragment_identities
            .windows(2)
            .any(|pair| pair[0] == pair[1])
        {
            return Err(OwnerValidationFailure {
                error: io::Error::new(
                    io::ErrorKind::InvalidData,
                    "active pending state contains duplicate frozen fragment identities",
                )
                .into(),
                class: PublicationFailureClass::TerminalIntegrity,
            });
        }
        candidate_store
            .validate_exact_fragment_identities(&active_fragment_identities)
            .map_err(|error| {
                let error = ChronoxideError::from(error);
                OwnerValidationFailure {
                    class: resource_or_integrity(&error),
                    error,
                }
            })?;

        let mut first_partition = None;
        for identity in &active_fragment_identities {
            let partition = identity.fragment_key().partition_key();
            match first_partition {
                None => {
                    first_partition = Some(partition);
                    stats.active_partitions_capped = 1;
                }
                Some(first) if first == partition => {}
                Some(_) => {
                    stats.active_partitions_capped = 2;
                    break;
                }
            }
        }
        if stats.active_partitions_capped <= 1 {
            // The forbidden state is simultaneous ownership of one canonical
            // identity by two distinct full `(topic, partition)` keys. With
            // zero or one active partition that state is impossible, so
            // rebuilding the per-series owner index cannot add evidence.
            stats.at_most_one_partition_fast_path = true;
            return Ok(stats);
        }

        let mut owners = BTreeMap::<u64, Vec<(SeriesRef, &PartitionKey)>>::new();
        for pending in self.pending.iter().filter(|pending| !pending.handed_off) {
            for key in pending.fragment.series_keys() {
                stats.run_keys_examined = stats.run_keys_examined.saturating_add(1);
                let series_id = catalog
                    .series_id(key.series)
                    .map_err(|error| OwnerValidationFailure {
                        error: error.into(),
                        class: PublicationFailureClass::TerminalIntegrity,
                    })?
                    .ok_or_else(|| OwnerValidationFailure {
                        error: io::Error::new(
                            io::ErrorKind::InvalidData,
                            "pending live series is absent from the candidate catalog",
                        )
                        .into(),
                        class: PublicationFailureClass::TerminalIntegrity,
                    })?;
                let bucket = owners.entry(series_id).or_default();
                let mut known_identity = false;
                for (known_series, owner) in bucket.iter().copied() {
                    stats.canonical_identity_comparisons =
                        stats.canonical_identity_comparisons.saturating_add(1);
                    if !catalog
                        .canonical_series_identity_eq(known_series, key.series)
                        .map_err(|error| OwnerValidationFailure {
                            error: error.into(),
                            class: PublicationFailureClass::TerminalIntegrity,
                        })?
                    {
                        continue;
                    }
                    known_identity = true;
                    if *owner != pending.partition {
                        return Err(OwnerValidationFailure {
                            error: io::Error::new(
                                io::ErrorKind::InvalidData,
                                format!(
                                    "canonical live series (raw refs {} and {}) is simultaneously owned by {} and {}",
                                    known_series.get(),
                                    key.series.get(),
                                    owner,
                                    pending.partition
                                ),
                            )
                            .into(),
                            class: PublicationFailureClass::HealableOwnerConflict,
                        });
                    }
                    break;
                }
                if !known_identity {
                    bucket
                        .try_reserve(1)
                        .map_err(|error| OwnerValidationFailure {
                            error: io::Error::new(io::ErrorKind::OutOfMemory, error).into(),
                            class: PublicationFailureClass::RetryableResource,
                        })?;
                    bucket.push((key.series, &pending.partition));
                }
            }
        }
        stats.id_buckets = u64::try_from(owners.len()).unwrap_or(u64::MAX);
        Ok(stats)
    }

    fn validate_coverage(
        &self,
        completed: &CompletedMessageCoverage,
    ) -> Result<RecordedSampleOrderSet> {
        let mut head_coverage = CoverageLedger::empty();
        let mut head_orders = RecordedSampleOrderSet::empty();
        let mut handoff_orders = RecordedSampleOrderSet::empty();
        for pending in &self.pending {
            if !pending.fragment.coverage_tracking_enabled() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "live coverage validation encountered an untracked fragment",
                )
                .into());
            }
            let range = pending.fragment.recorded_order_range().ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "tracked pending fragment has no recorded order range",
                )
            })?;
            if range.last().message_sequence() > completed.message_sequence {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "pending fragment extends beyond the candidate message cut",
                )
                .into());
            }
            let exact = pending.fragment.recorded_orders();
            exact.validate()?;
            if exact.sample_count() != pending.fragment.datapoints()
                || exact.sample_count() != pending.fragment.coverage().sample_count()
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "frozen fragment exact ownership disagrees with its datapoints or ledger",
                )
                .into());
            }
            if exact.runs().first().map(|run| run.first()) != Some(range.first())
                || exact.runs().last().map(|run| run.last()) != Some(range.last())
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "frozen fragment coarse order range disagrees with exact ownership",
                )
                .into());
            }
            if pending.handed_off {
                handoff_orders = handoff_orders.checked_union(exact)?;
            } else {
                head_orders = head_orders.checked_union(exact)?;
                head_coverage = head_coverage.checked_merge(pending.fragment.coverage())?;
            }
        }

        // This is the exact structural cut proof. Pairwise overlap is rejected
        // by each checked union; equality rejects every missing, duplicated, or
        // reordered successful order regardless of the commutative digest.
        let candidate_unsealed = head_orders.checked_union(&handoff_orders)?;
        if candidate_unsealed != self.expected_unsealed {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "live exact ownership mismatch: expected {} samples in {} runs, structurally owned {} samples in {} runs",
                    self.expected_unsealed.sample_count(),
                    self.expected_unsealed.run_count(),
                    candidate_unsealed.sample_count(),
                    candidate_unsealed.run_count(),
                ),
            )
            .into());
        }
        let owned = self.sealed_coverage.checked_merge(head_coverage)?;
        if owned != completed.completed_prefix {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "live coverage mismatch: expected {} recorded samples, structurally owned {}",
                    completed.completed_prefix.sample_count(),
                    owned.sample_count()
                ),
            )
            .into());
        }
        Ok(head_orders)
    }

    fn prepare_final_empty_head_proof(
        &self,
        seal_everything: bool,
        heads: &HashMap<PartitionKey, PartitionHead>,
        candidate_expected_unsealed: &RecordedSampleOrderSet,
        candidate_manifest_cut: &ManifestCut,
        candidate_sealed: &SegmentStoreReader,
        inventory_refreshed: bool,
    ) -> Result<Option<FinalEmptyHeadProof>> {
        if !seal_everything {
            return Ok(None);
        }
        if candidate_sealed.validated_manifest_cut() != Some(candidate_manifest_cut) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "final sealed-only proof is not bound to its candidate manifest cut",
            )
            .into());
        }
        if !self.pending.is_empty() && !inventory_refreshed {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "final sealed-only proof did not refresh the handed-off inventory",
            )
            .into());
        }
        if !candidate_expected_unsealed.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "final sealed-only publication retained exact head-owned sample orders",
            )
            .into());
        }
        if self.pending.iter().any(|pending| !pending.handed_off) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "final sealed-only publication retained a non-handed-off fragment",
            )
            .into());
        }
        if !self.seal_attempts.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "final sealed-only publication retained an incomplete seal attempt",
            )
            .into());
        }
        if heads
            .values()
            .any(|state| state.head.publication_fragment_count() != 0)
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "final sealed-only publication retained mutable head samples after freeze",
            )
            .into());
        }

        let committed_count = self
            .pending
            .iter()
            .filter(|pending| pending.committed)
            .count();
        let mut committed_fragment_identities = Vec::new();
        committed_fragment_identities
            .try_reserve_exact(committed_count)
            .map_err(|error| io::Error::new(io::ErrorKind::OutOfMemory, error))?;
        for pending in self.pending.iter().filter(|pending| pending.committed) {
            committed_fragment_identities.push(self.pending_fragment_identity(pending)?);
        }
        committed_fragment_identities.sort_unstable();
        if committed_fragment_identities
            .windows(2)
            .any(|pair| pair[0] == pair[1])
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "final sealed-only publication found duplicate committed fragment identities",
            )
            .into());
        }
        Ok(Some(FinalEmptyHeadProof {
            committed_fragment_identities,
        }))
    }

    fn build_candidate_sample_store(
        &self,
        final_empty_head: Option<&FinalEmptyHeadProof>,
    ) -> Result<LiveSampleStore> {
        let mut builder = LiveSampleStoreBuilder::from_store(&self.sample_store);
        if let Some(proof) = final_empty_head {
            builder.clear_if_exact_fragments(&proof.committed_fragment_identities)?;
            return Ok(builder.finish());
        }
        let mut retire = BTreeSet::new();
        for pending in self.pending.iter().filter(|pending| pending.handed_off) {
            retire.insert(pending.fragment_key()?);
        }
        for key in retire {
            builder.remove_fragment_key(&key)?;
        }
        #[cfg(test)]
        self.run_publication_hook(PublicationStage::SampleRetirementPathsReady);

        let mut additions = self
            .pending
            .iter()
            .filter(|pending| !pending.handed_off && !pending.committed)
            .collect::<Vec<_>>();
        additions.sort_by_key(|pending| {
            let range = pending
                .fragment
                .recorded_order_range()
                .expect("tracked fragment was validated before candidate construction");
            (
                pending.partition.clone(),
                pending.fragment.start_ms(),
                pending.fragment.end_ms(),
                pending.fragment.lane(),
                range.first(),
                range.last(),
            )
        });
        for pending in additions {
            if pending.memory_charge.is_none() {
                return Err(io::Error::new(
                    io::ErrorKind::OutOfMemory,
                    "pending frozen fragment has not passed live-memory admission",
                )
                .into());
            }
            let identity = FrozenFragmentIdentity::for_fragment(
                LivePartitionKey::new(
                    Arc::<str>::from(pending.partition.topic.as_str()),
                    pending.partition.partition,
                ),
                pending.fragment.as_ref(),
            )?;
            builder.insert_fragment(identity, Arc::clone(&pending.fragment))?;
        }
        #[cfg(test)]
        self.run_publication_hook(PublicationStage::SampleDescriptorPathsReady);
        Ok(builder.finish())
    }

    fn prepare_handed_off_retirements(
        &self,
        heads: &HashMap<PartitionKey, PartitionHead>,
    ) -> Result<Vec<HandoffRetirement>> {
        let mut retired = BTreeSet::new();
        for pending in self.pending.iter().filter(|pending| pending.handed_off) {
            let still_head_owned = self.pending.iter().any(|candidate| {
                !candidate.handed_off
                    && candidate.partition == pending.partition
                    && candidate.fragment.start_ms() == pending.fragment.start_ms()
                    && candidate.fragment.end_ms() == pending.fragment.end_ms()
                    && candidate.fragment.lane() == pending.fragment.lane()
            });
            if still_head_owned {
                continue;
            }
            retired.insert(HandoffRetirement {
                partition: pending.partition.clone(),
                start_ms: pending.fragment.start_ms(),
                end_ms: pending.fragment.end_ms(),
                lane: pending.fragment.lane(),
            });
        }
        for retirement in &retired {
            let state = heads.get(&retirement.partition).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "handed-off fragment lost its source partition head",
                )
            })?;
            state.head.validate_kind_guard_retirement(
                retirement.start_ms,
                retirement.end_ms,
                retirement.lane,
            )?;
        }
        Ok(retired.into_iter().collect())
    }

    fn apply_handed_off_retirements(
        &mut self,
        heads: &mut HashMap<PartitionKey, PartitionHead>,
        retirements: &[HandoffRetirement],
    ) {
        for retirement in retirements {
            let state = heads
                .get_mut(&retirement.partition)
                .expect("handoff retirement partition was validated before root commit");
            state
                .head
                .retire_kind_guards(retirement.start_ms, retirement.end_ms, retirement.lane)
                .expect(
                    "handoff retirement was validated immediately before the single-writer commit",
                );
            if retirement.lane == FrozenHeadLane::InOrder {
                state
                    .seal_ready_ranges
                    .remove(&(retirement.start_ms, retirement.end_ms));
            }
        }
        self.pending.retain(|pending| !pending.handed_off);
    }

    fn fail_readiness(
        &mut self,
        stage: &'static str,
        error: &ChronoxideError,
        class: PublicationFailureClass,
    ) {
        match class {
            PublicationFailureClass::RetryableResource => {}
            PublicationFailureClass::HealableOwnerConflict => {
                self.owner_conflict_seen = true;
            }
            PublicationFailureClass::TerminalIntegrity => {
                if self.earliest_terminal_failure.is_none() {
                    self.earliest_terminal_failure = Some(RetainedPublicationFailure {
                        stage,
                        message: Arc::from(error.to_string()),
                    });
                }
            }
        }
        let _ = self
            .handle
            .mark_failed(Arc::<str>::from(format!("{stage}: {error}")));
        error!(
            stage,
            failure_class = ?class,
            error = %error,
            "Live view publication failed"
        );
    }

    #[cfg(test)]
    fn set_publication_hook(&mut self, hook: impl Fn(PublicationStage) + Send + Sync + 'static) {
        self.publication_hook = Some(Arc::new(hook));
    }

    #[cfg(test)]
    pub(super) fn set_next_head_decode_hook(&mut self, hook: impl Fn() + Send + Sync + 'static) {
        self.next_head_decode_hook = Some(Arc::new(hook));
    }

    #[cfg(test)]
    fn run_publication_hook(&self, stage: PublicationStage) {
        if let Some(hook) = &self.publication_hook {
            hook(stage);
        }
    }
}

fn duration_ns(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

fn resource_or_integrity(error: &ChronoxideError) -> PublicationFailureClass {
    match error.kind() {
        crate::error::ErrorKind::IoError(source) if source.kind() == io::ErrorKind::OutOfMemory => {
            PublicationFailureClass::RetryableResource
        }
        _ => PublicationFailureClass::TerminalIntegrity,
    }
}

fn live_view_error(error: impl std::error::Error + Send + Sync + 'static) -> ChronoxideError {
    io::Error::other(error).into()
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};
    use std::sync::{Barrier, Mutex};
    use std::thread;

    use chronoxide_core::labels::METRIC_NAME_LABEL;
    use chronoxide_core::storage::head::{FloatEncoding, HeadConfig, IntEncoding, SampleValue};
    use chronoxide_core::storage::manifest::{
        manifest_file_name, read_manifest_inventory, write_current,
    };
    use chronoxide_core::storage::segment::SegmentSelector;

    fn publisher_config(interval: Duration) -> LivePublisherConfig {
        LivePublisherConfig {
            publish_interval: interval,
            max_view_staleness: Duration::from_secs(60),
            memory_admission_bytes: 64 * 1024 * 1024,
        }
    }

    fn publisher_writer_config(root: &std::path::Path) -> SegmentWriterConfig {
        SegmentWriterConfig::new(root, Duration::from_secs(10)).with_deterministic_segment_ids(81)
    }

    fn publisher(root: &std::path::Path, interval: Duration) -> LivePublisher {
        LivePublisher::new(publisher_config(interval), publisher_writer_config(root)).unwrap()
    }

    fn labelsets() -> (LabelSetInterner, SeriesRef) {
        let mut labelsets = LabelSetInterner::new_versioned_flat();
        let mut stats = OtlpMetricsIngestionStats::new();
        let series = labelsets
            .intern(
                &[
                    KeyValueRef::from((METRIC_NAME_LABEL, "live_metric")),
                    KeyValueRef::from(("host", "a")),
                ],
                &mut stats,
            )
            .unwrap();
        (labelsets, series)
    }

    fn partition_head() -> PartitionHead {
        let mut head = HeadBuffer::new(
            HeadConfig::new(
                Duration::from_secs(10),
                FloatEncoding::Gorilla,
                IntEncoding::DeltaZigZag,
            )
            .with_out_of_order_time_window(Duration::from_secs(20)),
        )
        .unwrap();
        head.enable_live_coverage_tracking().unwrap();
        PartitionHead {
            head,
            stats: HeadBufferStats::new(),
            seal_ready_ranges: BTreeSet::new(),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn record(
        head: &mut PartitionHead,
        message_sequence: u64,
        ordinal: u64,
        series: SeriesRef,
        timestamp_ms: u64,
        value: f64,
        message_coverage: &mut CoverageLedger,
        completed_prefix: &mut CoverageLedger,
    ) {
        let order = RecordedSampleOrder::new(MessageSequence::new(message_sequence), ordinal);
        let contribution = RecordedSampleContribution::for_sample(
            order,
            series,
            timestamp_ms,
            &SampleValue::Float(value),
            &mut Vec::new(),
        )
        .unwrap();
        let retained_window_slot = head
            .head
            .try_reserve_retained_window_for_publication()
            .unwrap();
        let outcome = head
            .head
            .record_sample_with_coverage(
                series,
                timestamp_ms,
                SampleValue::Float(value),
                contribution,
            )
            .unwrap();
        assert!(outcome.recorded);
        *message_coverage = message_coverage
            .checked_merge(contribution.ledger())
            .unwrap();
        *completed_prefix = completed_prefix
            .checked_merge(contribution.ledger())
            .unwrap();
        if let Some(window) = outcome.completed_window {
            let range = (window.start_ms, window.end_ms);
            head.head
                .retain_completed_window_for_publication(retained_window_slot, window)
                .unwrap();
            head.seal_ready_ranges.insert(range);
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn record_for_publication(
        publisher: &mut LivePublisher,
        head: &mut PartitionHead,
        message_sequence: u64,
        ordinal: u64,
        series: SeriesRef,
        timestamp_ms: u64,
        value: f64,
        message_coverage: &mut CoverageLedger,
        completed_prefix: &mut CoverageLedger,
    ) {
        publisher.reserve_expected_order_slot().unwrap();
        record(
            head,
            message_sequence,
            ordinal,
            series,
            timestamp_ms,
            value,
            message_coverage,
            completed_prefix,
        );
    }

    fn completed(
        sequence: u64,
        message_coverage: CoverageLedger,
        completed_prefix: CoverageLedger,
    ) -> CompletedMessageCoverage {
        let ordinals = (0..message_coverage.sample_count()).collect::<Vec<_>>();
        completed_with_ordinals(
            sequence,
            message_coverage,
            completed_prefix,
            ordinals.as_slice(),
        )
    }

    fn completed_with_ordinals(
        sequence: u64,
        message_coverage: CoverageLedger,
        completed_prefix: CoverageLedger,
        ordinals: &[u64],
    ) -> CompletedMessageCoverage {
        assert_eq!(
            u64::try_from(ordinals.len()).unwrap(),
            message_coverage.sample_count()
        );
        let mut successful_orders = RecordedSampleOrderSet::empty();
        for &ordinal in ordinals {
            successful_orders
                .try_append_order(RecordedSampleOrder::new(
                    MessageSequence::new(sequence),
                    ordinal,
                ))
                .unwrap();
        }
        CompletedMessageCoverage {
            message_sequence: MessageSequence::new(sequence),
            coverage: message_coverage,
            completed_prefix,
            successful_orders,
        }
    }

    fn query(view: &LiveQueryView<LiveStorageView>) -> Vec<(u64, f64)> {
        let mut session = view
            .payload()
            .sealed()
            .query_session_with_head_view(view.payload().head())
            .unwrap();
        session
            .query_selector(&SegmentSelector::metric("live_metric"), 0, 30_000)
            .unwrap()
            .into_iter()
            .flat_map(|result| result.samples)
            .collect()
    }

    #[derive(Debug, Clone, Copy)]
    enum AtomicPublicationPath {
        HeadOnly,
        ManifestHandoff,
    }

    fn assert_atomic_visibility_at_stage(stage: PublicationStage, path: AtomicPublicationPath) {
        let root = tempfile::tempdir().unwrap();
        let mut publisher = publisher(root.path(), Duration::from_nanos(1));
        let handle = publisher.handle();
        let (mut labelsets, series) = labelsets();
        let partition = PartitionKey::new("metrics", 0);
        let mut heads = HashMap::from([(partition, partition_head())]);
        let mut prefix = CoverageLedger::empty();

        let mut first = CoverageLedger::empty();
        record_for_publication(
            &mut publisher,
            heads.values_mut().next().unwrap(),
            1,
            0,
            series,
            1_000,
            1.0,
            &mut first,
            &mut prefix,
        );
        publisher
            .on_message_boundary(
                MessageSequence::new(1),
                completed(1, first, prefix),
                &mut heads,
                &mut labelsets,
            )
            .unwrap();

        let second_timestamp = match path {
            AtomicPublicationPath::HeadOnly => 2_000,
            AtomicPublicationPath::ManifestHandoff => 11_000,
        };
        let mut second = CoverageLedger::empty();
        record_for_publication(
            &mut publisher,
            heads.values_mut().next().unwrap(),
            2,
            0,
            series,
            second_timestamp,
            2.0,
            &mut second,
            &mut prefix,
        );

        let (entered_tx, entered_rx) = std::sync::mpsc::channel();
        let release = Arc::new(Barrier::new(2));
        let release_for_hook = Arc::clone(&release);
        publisher.set_publication_hook(move |observed| {
            if observed == stage {
                entered_tx.send(()).unwrap();
                release_for_hook.wait();
            }
        });

        let publisher_thread = thread::spawn(move || {
            publisher
                .on_message_boundary(
                    MessageSequence::new(2),
                    completed(2, second, prefix),
                    &mut heads,
                    &mut labelsets,
                )
                .unwrap();
        });
        entered_rx
            .recv_timeout(Duration::from_secs(10))
            .unwrap_or_else(|error| {
                panic!("publication did not reach {stage:?} on {path:?}: {error}")
            });

        let paused = handle.pin(Instant::now()).unwrap();
        if stage == PublicationStage::RootSwapped {
            assert_eq!(paused.generation(), 2, "stage={stage:?} path={path:?}");
            assert_eq!(
                query(&paused),
                vec![(1_000, 1.0), (second_timestamp, 2.0)],
                "stage={stage:?} path={path:?}"
            );
        } else {
            assert_eq!(paused.generation(), 1, "stage={stage:?} path={path:?}");
            assert_eq!(
                query(&paused),
                vec![(1_000, 1.0)],
                "stage={stage:?} path={path:?}"
            );
        }

        release.wait();
        publisher_thread.join().unwrap();
        let committed = handle.pin(Instant::now()).unwrap();
        assert_eq!(committed.generation(), 2, "stage={stage:?} path={path:?}");
        assert_eq!(
            query(&committed),
            vec![(1_000, 1.0), (second_timestamp, 2.0)],
            "stage={stage:?} path={path:?}"
        );
        assert_eq!(
            matches!(committed.manifest_cut(), ManifestCut::Present { .. }),
            matches!(path, AtomicPublicationPath::ManifestHandoff),
            "stage={stage:?} path={path:?}"
        );
    }

    #[test]
    fn every_publication_stage_exposes_only_the_old_or_complete_new_root() {
        let common_stages = [
            PublicationStage::FrozenInputsRetained,
            PublicationStage::InventoryReady,
            PublicationStage::CoverageValidated,
            PublicationStage::SampleRetirementPathsReady,
            PublicationStage::SampleDescriptorPathsReady,
            PublicationStage::SampleRootReady,
            PublicationStage::CatalogPagesReady,
            PublicationStage::CatalogPostingsReady,
            PublicationStage::CatalogReady,
            PublicationStage::OwnersValidated,
            PublicationStage::HandoffRetirementReady,
            PublicationStage::CommitDescriptorReady,
            PublicationStage::RootSwapped,
        ];
        for path in [
            AtomicPublicationPath::HeadOnly,
            AtomicPublicationPath::ManifestHandoff,
        ] {
            for stage in common_stages {
                assert_atomic_visibility_at_stage(stage, path);
            }
        }
        assert_atomic_visibility_at_stage(
            PublicationStage::ManifestHandoffCommitted,
            AtomicPublicationPath::ManifestHandoff,
        );
    }

    #[test]
    fn commit_descriptor_failure_retries_exactly_at_a_later_message_boundary() {
        let root = tempfile::tempdir().unwrap();
        let mut publisher = publisher(root.path(), Duration::from_nanos(1));
        let handle = publisher.handle();
        let (mut labelsets, series) = labelsets();
        let partition = PartitionKey::new("metrics", 0);
        let mut heads = HashMap::from([(partition, partition_head())]);
        let mut prefix = CoverageLedger::empty();

        let mut first = CoverageLedger::empty();
        record_for_publication(
            &mut publisher,
            heads.values_mut().next().unwrap(),
            1,
            0,
            series,
            1_000,
            1.0,
            &mut first,
            &mut prefix,
        );
        publisher
            .on_message_boundary(
                MessageSequence::new(1),
                completed(1, first, prefix),
                &mut heads,
                &mut labelsets,
            )
            .unwrap();
        let pinned_first = handle.pin(Instant::now()).unwrap();

        let mut second = CoverageLedger::empty();
        record_for_publication(
            &mut publisher,
            heads.values_mut().next().unwrap(),
            2,
            0,
            series,
            11_000,
            2.0,
            &mut second,
            &mut prefix,
        );
        let kind_guards_before_failure = heads
            .values()
            .next()
            .expect("one partition")
            .head
            .kind_guard_count();
        assert_eq!(kind_guards_before_failure, 2);
        publisher.fail_next_commit_descriptor = true;
        let error = publisher
            .on_message_boundary(
                MessageSequence::new(2),
                completed(2, second, prefix),
                &mut heads,
                &mut labelsets,
            )
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("injected live commit-descriptor preparation failure")
        );
        assert_eq!(query(&pinned_first), vec![(1_000, 1.0)]);
        assert!(matches!(
            handle.pin(Instant::now()),
            Err(chronoxide_core::storage::live_view::LiveViewError::Failed(
                _
            ))
        ));
        assert_eq!(publisher.expected_unsealed.sample_count(), 2);
        assert!(
            publisher.pending.iter().any(|pending| pending.handed_off),
            "the sealed range remains publisher-owned until the root swap"
        );
        let head_after_failure = heads.values().next().expect("one partition");
        assert_eq!(
            head_after_failure.head.kind_guard_count(),
            kind_guards_before_failure,
            "retirement preparation must not mutate kind guards"
        );
        assert!(
            head_after_failure.seal_ready_ranges.contains(&(0, 10_000)),
            "the failed root swap must not retire the seal-ready range"
        );
        assert_eq!(
            publisher
                .pending
                .iter()
                .filter(|pending| !pending.committed)
                .count(),
            1,
            "the failed candidate must leave its second-message input pending"
        );

        // A genuinely later boundary incorporates both the abandoned
        // candidate's exact input and this new message. Generation 2 must be
        // complete, with no skipped generation and no duplicated sample.
        let mut third = CoverageLedger::empty();
        record_for_publication(
            &mut publisher,
            heads.values_mut().next().unwrap(),
            3,
            0,
            series,
            12_000,
            3.0,
            &mut third,
            &mut prefix,
        );
        publisher
            .on_message_boundary(
                MessageSequence::new(3),
                completed(3, third, prefix),
                &mut heads,
                &mut labelsets,
            )
            .unwrap();

        let recovered = handle.pin(Instant::now()).unwrap();
        assert_eq!(recovered.generation(), 2);
        assert_eq!(recovered.visible_message_sequence(), 3);
        assert_eq!(
            query(&recovered),
            vec![(1_000, 1.0), (11_000, 2.0), (12_000, 3.0)]
        );
        assert_eq!(query(&pinned_first), vec![(1_000, 1.0)]);
        assert_eq!(publisher.expected_unsealed.sample_count(), 2);
        assert!(publisher.pending.iter().all(|pending| pending.committed));
        assert!(publisher.pending.iter().all(|pending| !pending.handed_off));
        let recovered_head = heads.values().next().expect("one partition");
        assert_eq!(recovered_head.head.kind_guard_count(), 1);
        assert!(!recovered_head.seal_ready_ranges.contains(&(0, 10_000)));
    }

    fn clone_pending_fragment(pending: &PendingFragment) -> PendingFragment {
        PendingFragment {
            partition: pending.partition.clone(),
            fragment: Arc::clone(&pending.fragment),
            estimated_bytes: pending.estimated_bytes,
            memory_charge: pending.memory_charge.as_ref().map(Arc::clone),
            committed: pending.committed,
            handed_off: pending.handed_off,
        }
    }

    #[test]
    fn sparse_exact_orders_split_across_ranges_and_lanes_validate_at_one_cut() {
        let root = tempfile::tempdir().unwrap();
        let mut publisher = publisher(root.path(), Duration::from_secs(60));
        let (_labelsets, series) = labelsets();
        let partition = PartitionKey::new("metrics", 0);
        let mut heads = HashMap::from([(partition, partition_head())]);
        let mut message = CoverageLedger::empty();
        let mut prefix = CoverageLedger::empty();

        // Ordinals 1 and 3 are rejected/omitted attempts. Their reservations
        // remain harmless, while exact successful membership is {0, 2, 4}.
        for _ in 0..=4 {
            publisher.reserve_expected_order_slot().unwrap();
        }
        record(
            heads.values_mut().next().unwrap(),
            1,
            0,
            series,
            1_000,
            1.0,
            &mut message,
            &mut prefix,
        );
        record(
            heads.values_mut().next().unwrap(),
            1,
            2,
            series,
            11_000,
            11.0,
            &mut message,
            &mut prefix,
        );
        record(
            heads.values_mut().next().unwrap(),
            1,
            4,
            series,
            10_500,
            10.5,
            &mut message,
            &mut prefix,
        );
        publisher.unregistered_completion =
            Some(completed_with_ordinals(1, message, prefix, &[0, 2, 4]));
        publisher.register_retained_completion().unwrap();
        publisher.freeze_heads(&mut heads).unwrap();

        assert_eq!(publisher.pending.len(), 3);
        assert_eq!(
            publisher
                .pending
                .iter()
                .map(|pending| (
                    pending.fragment.start_ms(),
                    pending.fragment.lane(),
                    pending.fragment.recorded_orders().sample_count(),
                ))
                .collect::<BTreeSet<_>>(),
            BTreeSet::from([
                (0, FrozenHeadLane::InOrder, 1),
                (10_000, FrozenHeadLane::InOrder, 1),
                (10_000, FrozenHeadLane::OutOfOrder, 1),
            ])
        );
        let remaining = publisher
            .validate_coverage(publisher.latest_completed.as_ref().unwrap())
            .unwrap();
        assert_eq!(remaining, publisher.expected_unsealed);
        assert_eq!(remaining.sample_count(), 3);
        assert_eq!(remaining.run_count(), 3);
    }

    #[test]
    fn duplicate_exact_owner_is_rejected_even_when_structural_count_matches() {
        let root = tempfile::tempdir().unwrap();
        let mut publisher = publisher(root.path(), Duration::from_secs(60));
        let (_labelsets, series) = labelsets();
        let partition = PartitionKey::new("metrics", 0);
        let mut heads = HashMap::from([(partition, partition_head())]);
        let mut message = CoverageLedger::empty();
        let mut prefix = CoverageLedger::empty();
        for _ in 0..=2 {
            publisher.reserve_expected_order_slot().unwrap();
        }
        record(
            heads.values_mut().next().unwrap(),
            1,
            0,
            series,
            1_000,
            1.0,
            &mut message,
            &mut prefix,
        );
        record(
            heads.values_mut().next().unwrap(),
            1,
            2,
            series,
            11_000,
            11.0,
            &mut message,
            &mut prefix,
        );
        publisher.unregistered_completion =
            Some(completed_with_ordinals(1, message, prefix, &[0, 2]));
        publisher.register_retained_completion().unwrap();
        publisher.freeze_heads(&mut heads).unwrap();
        assert_eq!(publisher.pending.len(), 2);

        publisher.pending[1] = clone_pending_fragment(&publisher.pending[0]);
        assert_eq!(
            publisher
                .pending
                .iter()
                .map(|pending| pending.fragment.coverage().sample_count())
                .sum::<u64>(),
            publisher.expected_unsealed.sample_count()
        );
        let error = publisher
            .validate_coverage(publisher.latest_completed.as_ref().unwrap())
            .unwrap_err();
        assert!(error.to_string().contains("more than one structural owner"));
    }

    #[test]
    fn missing_exact_order_plus_unrelated_replacement_is_rejected() {
        let root = tempfile::tempdir().unwrap();
        let mut publisher = publisher(root.path(), Duration::from_secs(60));
        let (_labelsets, series) = labelsets();
        let partition = PartitionKey::new("metrics", 0);
        let mut heads = HashMap::from([(partition.clone(), partition_head())]);
        let mut message = CoverageLedger::empty();
        let mut prefix = CoverageLedger::empty();
        for _ in 0..=2 {
            publisher.reserve_expected_order_slot().unwrap();
        }
        record(
            heads.get_mut(&partition).unwrap(),
            1,
            0,
            series,
            1_000,
            1.0,
            &mut message,
            &mut prefix,
        );
        record(
            heads.get_mut(&partition).unwrap(),
            1,
            2,
            series,
            11_000,
            11.0,
            &mut message,
            &mut prefix,
        );
        publisher.unregistered_completion =
            Some(completed_with_ordinals(1, message, prefix, &[0, 2]));
        publisher.register_retained_completion().unwrap();
        publisher.freeze_heads(&mut heads).unwrap();

        let mut replacement = partition_head();
        let mut replacement_message = CoverageLedger::empty();
        let mut replacement_prefix = CoverageLedger::empty();
        record(
            &mut replacement,
            1,
            1,
            series,
            21_000,
            21.0,
            &mut replacement_message,
            &mut replacement_prefix,
        );
        let replacement = replacement
            .head
            .try_freeze_for_publication()
            .unwrap()
            .pop()
            .unwrap();
        publisher.pending[1].fragment = Arc::new(replacement);
        assert_eq!(
            publisher
                .pending
                .iter()
                .map(|pending| pending.fragment.coverage().sample_count())
                .sum::<u64>(),
            publisher.expected_unsealed.sample_count()
        );

        let error = publisher
            .validate_coverage(publisher.latest_completed.as_ref().unwrap())
            .unwrap_err();
        assert!(error.to_string().contains("exact ownership mismatch"));
    }

    #[test]
    fn failed_exact_registration_is_retained_and_merged_before_later_boundary() {
        let root = tempfile::tempdir().unwrap();
        let mut publisher = publisher(root.path(), Duration::from_secs(60));
        let handle = publisher.handle();
        let (mut labelsets, series) = labelsets();
        let partition = PartitionKey::new("metrics", 0);
        let mut heads = HashMap::from([(partition, partition_head())]);
        let mut prefix = CoverageLedger::empty();

        let mut first = CoverageLedger::empty();
        record_for_publication(
            &mut publisher,
            heads.values_mut().next().unwrap(),
            1,
            0,
            series,
            1_000,
            1.0,
            &mut first,
            &mut prefix,
        );
        publisher.fail_next_expected_registration = true;
        assert!(
            publisher
                .on_message_boundary(
                    MessageSequence::new(1),
                    completed(1, first, prefix),
                    &mut heads,
                    &mut labelsets,
                )
                .is_err()
        );
        assert!(publisher.latest_completed.is_none());
        assert!(publisher.unregistered_completion.is_some());
        assert!(publisher.expected_unsealed.is_empty());

        // This is the admission step performed by the next acquired-message
        // hook. It must merge the retained first completion before order 2:0
        // can be admitted.
        publisher.prepare_for_next_message().unwrap();
        assert_eq!(
            publisher
                .latest_completed
                .as_ref()
                .unwrap()
                .message_sequence,
            MessageSequence::new(1)
        );
        assert_eq!(publisher.expected_unsealed.sample_count(), 1);
        assert!(publisher.unregistered_completion.is_none());

        let mut second = CoverageLedger::empty();
        record_for_publication(
            &mut publisher,
            heads.values_mut().next().unwrap(),
            2,
            0,
            series,
            2_000,
            2.0,
            &mut second,
            &mut prefix,
        );
        publisher
            .on_message_boundary(
                MessageSequence::new(2),
                completed(2, second, prefix),
                &mut heads,
                &mut labelsets,
            )
            .unwrap();

        assert_eq!(publisher.expected_unsealed.sample_count(), 2);
        assert_eq!(
            publisher
                .latest_completed
                .as_ref()
                .unwrap()
                .message_sequence,
            MessageSequence::new(2)
        );
        assert_eq!(
            query(&handle.pin(Instant::now()).unwrap()),
            vec![(1_000, 1.0), (2_000, 2.0)]
        );
    }

    #[test]
    fn initial_head_only_generation_exposes_the_complete_message() {
        let root = tempfile::tempdir().unwrap();
        let mut publisher = publisher(root.path(), Duration::from_secs(60));
        let handle = publisher.handle();
        let (mut labelsets, series) = labelsets();
        let partition = PartitionKey::new("metrics", 0);
        let mut heads = HashMap::from([(partition, partition_head())]);

        let mut prefix = CoverageLedger::empty();
        let mut message = CoverageLedger::empty();
        record_for_publication(
            &mut publisher,
            heads.values_mut().next().unwrap(),
            1,
            0,
            series,
            1_000,
            1.0,
            &mut message,
            &mut prefix,
        );
        record_for_publication(
            &mut publisher,
            heads.values_mut().next().unwrap(),
            1,
            1,
            series,
            2_000,
            2.0,
            &mut message,
            &mut prefix,
        );
        assert!(handle.pin(Instant::now()).is_err());

        publisher
            .on_message_boundary(
                MessageSequence::new(1),
                completed(1, message, prefix),
                &mut heads,
                &mut labelsets,
            )
            .unwrap();

        let view = handle.pin(Instant::now()).unwrap();
        assert_eq!(view.generation(), 1);
        assert_eq!(view.visible_message_sequence(), 1);
        assert_eq!(query(&view), vec![(1_000, 1.0), (2_000, 2.0)]);
        assert_eq!(view.manifest_cut(), &ManifestCut::Absent);
    }

    #[test]
    fn no_data_shutdown_publishes_validated_empty_sealed_only_generation() {
        let root = tempfile::tempdir().unwrap();
        let mut publisher = publisher(root.path(), Duration::from_secs(60));
        let handle = publisher.handle();
        let mut labelsets = LabelSetInterner::new_versioned_flat();
        let mut heads = HashMap::new();
        let stages = Arc::new(Mutex::new(Vec::new()));
        let stages_for_hook = Arc::clone(&stages);
        publisher.set_publication_hook(move |stage| {
            stages_for_hook.lock().unwrap().push(stage);
        });

        assert!(handle.pin(Instant::now()).is_err());
        publisher.shutdown(&mut heads, &mut labelsets).unwrap();

        let view = handle.pin(Instant::now()).unwrap();
        assert_eq!(view.generation(), 1);
        assert_eq!(view.visible_message_sequence(), 0);
        assert_eq!(view.manifest_cut(), &ManifestCut::Absent);
        assert!(view.payload().head().is_empty());
        assert!(query(&view).is_empty());
        assert!(
            read_manifest_inventory(root.path().join("manifest"))
                .unwrap()
                .is_none()
        );
        let stages = stages.lock().unwrap();
        assert!(stages.contains(&PublicationStage::FinalEmptyRootsReady));
        assert!(!stages.contains(&PublicationStage::SampleRetirementPathsReady));
        assert!(!stages.contains(&PublicationStage::CatalogPostingsReady));
    }

    #[test]
    fn final_empty_fast_path_is_atomic_and_keeps_the_pinned_predecessor_queryable() {
        let root = tempfile::tempdir().unwrap();
        let mut publisher = publisher(root.path(), Duration::from_nanos(1));
        let handle = publisher.handle();
        let (mut labelsets, series) = labelsets();
        let partition = PartitionKey::new("metrics", 0);
        let mut heads = HashMap::from([(partition, partition_head())]);
        let mut prefix = CoverageLedger::empty();
        let mut message = CoverageLedger::empty();
        record_for_publication(
            &mut publisher,
            heads.values_mut().next().unwrap(),
            1,
            0,
            series,
            1_000,
            1.0,
            &mut message,
            &mut prefix,
        );
        publisher
            .on_message_boundary(
                MessageSequence::new(1),
                completed(1, message, prefix),
                &mut heads,
                &mut labelsets,
            )
            .unwrap();
        let pinned_predecessor = handle.pin(Instant::now()).unwrap();
        assert_eq!(pinned_predecessor.generation(), 1);
        assert_eq!(query(&pinned_predecessor), vec![(1_000, 1.0)]);

        let stages = Arc::new(Mutex::new(Vec::new()));
        let stages_for_hook = Arc::clone(&stages);
        let (candidate_ready_tx, candidate_ready_rx) = std::sync::mpsc::channel();
        let release = Arc::new(Barrier::new(2));
        let release_for_hook = Arc::clone(&release);
        publisher.set_publication_hook(move |stage| {
            stages_for_hook.lock().unwrap().push(stage);
            if stage == PublicationStage::FinalEmptyRootsReady {
                candidate_ready_tx.send(()).unwrap();
                release_for_hook.wait();
            }
        });

        let shutdown = thread::spawn(move || {
            publisher.shutdown(&mut heads, &mut labelsets).unwrap();
            (publisher, heads)
        });
        candidate_ready_rx
            .recv_timeout(Duration::from_secs(10))
            .expect("shutdown did not construct the proven empty roots");

        let before_swap = handle.pin(Instant::now()).unwrap();
        assert_eq!(before_swap.generation(), 1);
        assert_eq!(before_swap.manifest_cut(), &ManifestCut::Absent);
        assert_eq!(query(&before_swap), vec![(1_000, 1.0)]);
        assert_eq!(query(&pinned_predecessor), vec![(1_000, 1.0)]);

        release.wait();
        let (publisher, heads) = shutdown.join().unwrap();
        let final_view = handle.pin(Instant::now()).unwrap();
        assert_eq!(final_view.generation(), 2);
        assert!(matches!(
            final_view.manifest_cut(),
            ManifestCut::Present { .. }
        ));
        assert!(final_view.payload().head().is_empty());
        assert_eq!(query(&final_view), vec![(1_000, 1.0)]);
        assert_eq!(
            query(&pinned_predecessor),
            vec![(1_000, 1.0)],
            "the predecessor retains its old head-owned bytes"
        );

        let stages = stages.lock().unwrap();
        assert!(stages.contains(&PublicationStage::FinalEmptyRootsReady));
        assert!(!stages.contains(&PublicationStage::SampleRetirementPathsReady));
        assert!(!stages.contains(&PublicationStage::SampleDescriptorPathsReady));
        assert!(!stages.contains(&PublicationStage::CatalogPostingsReady));
        assert!(stages.contains(&PublicationStage::RootSwapped));
        assert!(publisher.pending.is_empty());
        assert!(publisher.sample_store.is_empty());
        assert_eq!(publisher.sample_store.fragment_count(), 0);
        assert!(publisher.expected_unsealed.is_empty());
        assert!(publisher.seal_attempts.is_empty());
        assert_eq!(publisher.catalog.as_ref().unwrap().active_series_len(), 0);
        assert!(heads.values().all(|state| {
            state.head.publication_fragment_count() == 0 && state.head.kind_guard_count() == 0
        }));
    }

    #[test]
    fn final_empty_fast_path_commit_failure_retains_ownership_and_retries_exactly() {
        let root = tempfile::tempdir().unwrap();
        let mut publisher = publisher(root.path(), Duration::from_nanos(1));
        let handle = publisher.handle();
        let (mut labelsets, series) = labelsets();
        let partition = PartitionKey::new("metrics", 0);
        let mut heads = HashMap::from([(partition, partition_head())]);
        let mut prefix = CoverageLedger::empty();
        let mut message = CoverageLedger::empty();
        record_for_publication(
            &mut publisher,
            heads.values_mut().next().unwrap(),
            1,
            0,
            series,
            1_000,
            1.0,
            &mut message,
            &mut prefix,
        );
        publisher
            .on_message_boundary(
                MessageSequence::new(1),
                completed(1, message, prefix),
                &mut heads,
                &mut labelsets,
            )
            .unwrap();
        let pinned_predecessor = handle.pin(Instant::now()).unwrap();

        let stages = Arc::new(Mutex::new(Vec::new()));
        let stages_for_hook = Arc::clone(&stages);
        publisher.set_publication_hook(move |stage| {
            stages_for_hook.lock().unwrap().push(stage);
        });
        publisher.fail_next_commit_descriptor = true;
        let error = publisher.shutdown(&mut heads, &mut labelsets).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("injected live commit-descriptor preparation failure")
        );
        assert_eq!(query(&pinned_predecessor), vec![(1_000, 1.0)]);
        assert!(matches!(
            handle.pin(Instant::now()),
            Err(chronoxide_core::storage::live_view::LiveViewError::Failed(
                _
            ))
        ));
        assert_eq!(publisher.sample_store.fragment_count(), 1);
        assert_eq!(publisher.catalog.as_ref().unwrap().active_series_len(), 1);
        assert_eq!(publisher.expected_unsealed.sample_count(), 1);
        assert_eq!(publisher.pending.len(), 1);
        assert!(publisher.pending[0].committed);
        assert!(publisher.pending[0].handed_off);
        assert_eq!(
            heads.values().next().unwrap().head.kind_guard_count(),
            1,
            "retirement remains unapplied before the root swap"
        );

        publisher.shutdown(&mut heads, &mut labelsets).unwrap();
        let final_view = handle.pin(Instant::now()).unwrap();
        assert_eq!(final_view.generation(), 2);
        assert!(final_view.payload().head().is_empty());
        assert_eq!(query(&final_view), vec![(1_000, 1.0)]);
        assert_eq!(query(&pinned_predecessor), vec![(1_000, 1.0)]);
        assert!(publisher.pending.is_empty());
        assert!(publisher.expected_unsealed.is_empty());
        assert!(publisher.sample_store.is_empty());
        assert_eq!(heads.values().next().unwrap().head.kind_guard_count(), 0);

        let stages = stages.lock().unwrap();
        assert_eq!(
            stages
                .iter()
                .filter(|&&stage| stage == PublicationStage::FinalEmptyRootsReady)
                .count(),
            2
        );
        assert!(!stages.contains(&PublicationStage::SampleRetirementPathsReady));
        assert!(!stages.contains(&PublicationStage::SampleDescriptorPathsReady));
        assert!(!stages.contains(&PublicationStage::CatalogPostingsReady));
        assert_eq!(
            stages
                .iter()
                .filter(|&&stage| stage == PublicationStage::RootSwapped)
                .count(),
            1
        );
    }

    #[test]
    fn final_empty_fast_path_rejects_committed_fragment_certificate_drift() {
        let root = tempfile::tempdir().unwrap();
        let mut publisher = publisher(root.path(), Duration::from_nanos(1));
        let handle = publisher.handle();
        let (mut labelsets, series) = labelsets();
        let partition = PartitionKey::new("metrics", 0);
        let mut heads = HashMap::from([(partition, partition_head())]);
        let mut prefix = CoverageLedger::empty();
        let mut message = CoverageLedger::empty();
        record_for_publication(
            &mut publisher,
            heads.values_mut().next().unwrap(),
            1,
            0,
            series,
            1_000,
            1.0,
            &mut message,
            &mut prefix,
        );
        publisher
            .on_message_boundary(
                MessageSequence::new(1),
                completed(1, message, prefix),
                &mut heads,
                &mut labelsets,
            )
            .unwrap();
        let pinned_predecessor = handle.pin(Instant::now()).unwrap();
        assert_eq!(publisher.pending.len(), 1);
        assert!(publisher.pending[0].committed);

        // Simulate publisher-metadata corruption without touching the
        // independently maintained sample-root certificate.
        publisher.pending[0].committed = false;
        let stages = Arc::new(Mutex::new(Vec::new()));
        let stages_for_hook = Arc::clone(&stages);
        publisher.set_publication_hook(move |stage| {
            stages_for_hook.lock().unwrap().push(stage);
        });
        let error = publisher.shutdown(&mut heads, &mut labelsets).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("does not exactly cover every committed fragment")
        );
        assert_eq!(query(&pinned_predecessor), vec![(1_000, 1.0)]);
        assert!(matches!(
            handle.pin(Instant::now()),
            Err(chronoxide_core::storage::live_view::LiveViewError::Failed(
                _
            ))
        ));
        assert_eq!(publisher.sample_store.fragment_count(), 1);
        assert_eq!(publisher.catalog.as_ref().unwrap().active_series_len(), 1);
        assert_eq!(publisher.expected_unsealed.sample_count(), 1);
        assert_eq!(publisher.pending.len(), 1);
        assert!(publisher.pending[0].handed_off);
        assert_eq!(heads.values().next().unwrap().head.kind_guard_count(), 1);
        let stages = stages.lock().unwrap();
        assert!(stages.contains(&PublicationStage::CoverageValidated));
        assert!(!stages.contains(&PublicationStage::FinalEmptyRootsReady));
        assert!(!stages.contains(&PublicationStage::RootSwapped));
    }

    #[test]
    fn coalesced_message_is_not_partially_visible_and_zero_record_cut_advances_on_force() {
        let root = tempfile::tempdir().unwrap();
        let mut publisher = publisher(root.path(), Duration::from_secs(60));
        let handle = publisher.handle();
        let (mut labelsets, series) = labelsets();
        let partition = PartitionKey::new("metrics", 0);
        let mut heads = HashMap::from([(partition, partition_head())]);
        let mut prefix = CoverageLedger::empty();

        let mut first = CoverageLedger::empty();
        record_for_publication(
            &mut publisher,
            heads.values_mut().next().unwrap(),
            1,
            0,
            series,
            1_000,
            1.0,
            &mut first,
            &mut prefix,
        );
        publisher
            .on_message_boundary(
                MessageSequence::new(1),
                completed(1, first, prefix),
                &mut heads,
                &mut labelsets,
            )
            .unwrap();
        let generation_one = handle.pin(Instant::now()).unwrap();

        let mut second = CoverageLedger::empty();
        record_for_publication(
            &mut publisher,
            heads.values_mut().next().unwrap(),
            2,
            0,
            series,
            2_000,
            2.0,
            &mut second,
            &mut prefix,
        );
        publisher
            .on_message_boundary(
                MessageSequence::new(2),
                completed(2, second, prefix),
                &mut heads,
                &mut labelsets,
            )
            .unwrap();
        let still_one = handle.pin(Instant::now()).unwrap();
        assert_eq!(still_one.generation(), 1);
        assert_eq!(still_one.visible_message_sequence(), 1);
        assert_eq!(query(&still_one), vec![(1_000, 1.0)]);
        assert_eq!(query(&generation_one), vec![(1_000, 1.0)]);

        publisher
            .on_message_boundary(
                MessageSequence::new(3),
                completed(3, CoverageLedger::empty(), prefix),
                &mut heads,
                &mut labelsets,
            )
            .unwrap();
        assert_eq!(handle.pin(Instant::now()).unwrap().generation(), 1);

        let stages = Arc::new(Mutex::new(Vec::new()));
        let stages_for_hook = Arc::clone(&stages);
        publisher.set_publication_hook(move |stage| {
            stages_for_hook.lock().unwrap().push(stage);
        });
        publisher.shutdown(&mut heads, &mut labelsets).unwrap();
        let final_view = handle.pin(Instant::now()).unwrap();
        assert_eq!(final_view.generation(), 2);
        assert_eq!(final_view.visible_message_sequence(), 3);
        assert_eq!(query(&final_view), vec![(1_000, 1.0), (2_000, 2.0)]);
        assert!(final_view.payload().head().is_empty());
        assert_eq!(
            query(&generation_one),
            vec![(1_000, 1.0)],
            "the mixed committed/uncommitted final handoff must preserve the pinned predecessor"
        );
        let stages = stages.lock().unwrap();
        assert!(stages.contains(&PublicationStage::FinalEmptyRootsReady));
        assert!(!stages.contains(&PublicationStage::SampleRetirementPathsReady));
        assert!(!stages.contains(&PublicationStage::SampleDescriptorPathsReady));
        assert!(!stages.contains(&PublicationStage::CatalogPostingsReady));
    }

    #[test]
    fn normal_and_postseal_ooo_handoffs_preserve_one_logical_result() {
        let root = tempfile::tempdir().unwrap();
        let mut publisher = publisher(root.path(), Duration::from_nanos(1));
        let handle = publisher.handle();
        let (mut labelsets, series) = labelsets();
        let partition = PartitionKey::new("metrics", 0);
        let mut heads = HashMap::from([(partition, partition_head())]);
        let mut prefix = CoverageLedger::empty();

        let mut message = CoverageLedger::empty();
        record_for_publication(
            &mut publisher,
            heads.values_mut().next().unwrap(),
            1,
            0,
            series,
            1_000,
            1.0,
            &mut message,
            &mut prefix,
        );
        publisher
            .on_message_boundary(
                MessageSequence::new(1),
                completed(1, message, prefix),
                &mut heads,
                &mut labelsets,
            )
            .unwrap();

        let mut message = CoverageLedger::empty();
        record_for_publication(
            &mut publisher,
            heads.values_mut().next().unwrap(),
            2,
            0,
            series,
            11_000,
            11.0,
            &mut message,
            &mut prefix,
        );
        publisher
            .on_message_boundary(
                MessageSequence::new(2),
                completed(2, message, prefix),
                &mut heads,
                &mut labelsets,
            )
            .unwrap();
        assert_eq!(
            query(&handle.pin(Instant::now()).unwrap()),
            vec![(1_000, 1.0), (11_000, 11.0)]
        );

        let mut message = CoverageLedger::empty();
        record_for_publication(
            &mut publisher,
            heads.values_mut().next().unwrap(),
            3,
            0,
            series,
            5_000,
            5.0,
            &mut message,
            &mut prefix,
        );
        publisher
            .on_message_boundary(
                MessageSequence::new(3),
                completed(3, message, prefix),
                &mut heads,
                &mut labelsets,
            )
            .unwrap();

        let view = handle.pin(Instant::now()).unwrap();
        assert_eq!(
            query(&view),
            vec![(1_000, 1.0), (5_000, 5.0), (11_000, 11.0)]
        );
        let inventory = read_manifest_inventory(root.path().join("manifest"))
            .unwrap()
            .unwrap();
        assert_eq!(inventory.segments.len(), 2);
        assert!(view.payload().head().samples().fragment_count() > 0);

        publisher.shutdown(&mut heads, &mut labelsets).unwrap();
        let final_view = handle.pin(Instant::now()).unwrap();
        assert!(final_view.payload().head().is_empty());
        assert_eq!(
            query(&final_view),
            vec![(1_000, 1.0), (5_000, 5.0), (11_000, 11.0)]
        );
        assert_eq!(
            query(&view),
            vec![(1_000, 1.0), (5_000, 5.0), (11_000, 11.0)],
            "the pre-shutdown generation retains its post-seal OOO head supplier"
        );
    }

    #[test]
    fn preseal_ooo_co_seals_normally_and_equal_timestamp_lww_survives_handoff() {
        let root = tempfile::tempdir().unwrap();
        let mut publisher = publisher(root.path(), Duration::from_nanos(1));
        let handle = publisher.handle();
        let (mut labelsets, series) = labelsets();
        let partition = PartitionKey::new("metrics", 0);
        let mut heads = HashMap::from([(partition, partition_head())]);
        let mut prefix = CoverageLedger::empty();

        for (sequence, timestamp_ms, value) in [(1, 6_000, 6.0), (2, 5_000, 5.0), (3, 6_000, 60.0)]
        {
            let mut message = CoverageLedger::empty();
            publisher.reserve_expected_order_slot().unwrap();
            record(
                heads.values_mut().next().unwrap(),
                sequence,
                0,
                series,
                timestamp_ms,
                value,
                &mut message,
                &mut prefix,
            );
            publisher
                .on_message_boundary(
                    MessageSequence::new(sequence),
                    completed(sequence, message, prefix),
                    &mut heads,
                    &mut labelsets,
                )
                .unwrap();
        }
        assert_eq!(
            query(&handle.pin(Instant::now()).unwrap()),
            vec![(5_000, 5.0), (6_000, 60.0)]
        );

        let mut message = CoverageLedger::empty();
        publisher.reserve_expected_order_slot().unwrap();
        record(
            heads.values_mut().next().unwrap(),
            4,
            0,
            series,
            11_000,
            11.0,
            &mut message,
            &mut prefix,
        );
        publisher
            .on_message_boundary(
                MessageSequence::new(4),
                completed(4, message, prefix),
                &mut heads,
                &mut labelsets,
            )
            .unwrap();

        let view = handle.pin(Instant::now()).unwrap();
        assert_eq!(
            query(&view),
            vec![(5_000, 5.0), (6_000, 60.0), (11_000, 11.0)]
        );
        let inventory = read_manifest_inventory(root.path().join("manifest"))
            .unwrap()
            .unwrap();
        assert_eq!(inventory.segments.len(), 1);
        let segment = root.path().join(&inventory.segments[0].segment_id);
        assert!(std::fs::metadata(segment.join("chunks.bin")).unwrap().len() > 0);
        assert_eq!(
            std::fs::metadata(segment.join("ooo_chunks.bin"))
                .unwrap()
                .len(),
            0
        );

        publisher.shutdown(&mut heads, &mut labelsets).unwrap();
        let final_view = handle.pin(Instant::now()).unwrap();
        assert!(final_view.payload().head().is_empty());
        assert_eq!(
            query(&final_view),
            vec![(5_000, 5.0), (6_000, 60.0), (11_000, 11.0)]
        );
        assert_eq!(
            query(&view),
            vec![(5_000, 5.0), (6_000, 60.0), (11_000, 11.0)],
            "the pre-shutdown generation retains its pre-seal OOO/LWW result"
        );
    }

    #[test]
    fn one_active_partition_skips_owner_index_without_losing_canonical_aliases() {
        let root = tempfile::tempdir().unwrap();
        let mut publisher = publisher(root.path(), Duration::from_nanos(1));
        let mut labelsets = LabelSetInterner::new_versioned_flat();
        let mut stats = OtlpMetricsIngestionStats::new();
        let raw_name = "a.label";
        let projected_name = normalize_label_name(raw_name);
        let raw = labelsets
            .intern(
                &[
                    KeyValueRef::from((METRIC_NAME_LABEL, "live_metric")),
                    KeyValueRef::from((raw_name, "same-value")),
                ],
                &mut stats,
            )
            .unwrap();
        let projected = labelsets
            .intern(
                &[
                    KeyValueRef::from((METRIC_NAME_LABEL, "live_metric")),
                    KeyValueRef::from((projected_name.as_str(), "same-value")),
                ],
                &mut stats,
            )
            .unwrap();
        assert_ne!(raw, projected);

        let partition = PartitionKey::new("topic-a", 7);
        let mut heads = HashMap::from([(partition.clone(), partition_head())]);
        let mut prefix = CoverageLedger::empty();
        let mut message = CoverageLedger::empty();
        record_for_publication(
            &mut publisher,
            heads.get_mut(&partition).unwrap(),
            1,
            0,
            raw,
            1_000,
            1.0,
            &mut message,
            &mut prefix,
        );
        record_for_publication(
            &mut publisher,
            heads.get_mut(&partition).unwrap(),
            1,
            1,
            projected,
            2_000,
            2.0,
            &mut message,
            &mut prefix,
        );
        publisher
            .on_message_boundary(
                MessageSequence::new(1),
                completed(1, message, prefix),
                &mut heads,
                &mut labelsets,
            )
            .unwrap();

        let owner_stats = publisher
            .validate_active_owners(
                publisher.catalog.as_deref().unwrap(),
                &publisher.sample_store,
            )
            .unwrap();
        assert_eq!(owner_stats.active_partitions_capped, 1);
        assert!(owner_stats.at_most_one_partition_fast_path);
        assert_eq!(owner_stats.run_keys_examined, 0);
        assert_eq!(owner_stats.id_buckets, 0);
        assert_eq!(owner_stats.canonical_identity_comparisons, 0);
        assert_eq!(
            query(&publisher.handle().pin(Instant::now()).unwrap()),
            vec![(1_000, 1.0), (2_000, 2.0)]
        );
    }

    #[test]
    fn owner_fast_path_rejects_pending_sample_fragment_certificate_drift() {
        let root = tempfile::tempdir().unwrap();
        let mut publisher = publisher(root.path(), Duration::from_nanos(1));
        let (mut labelsets, series) = labelsets();
        let partition = PartitionKey::new("topic-a", 7);
        let mut heads = HashMap::from([(partition.clone(), partition_head())]);
        let mut prefix = CoverageLedger::empty();
        let mut message = CoverageLedger::empty();
        record_for_publication(
            &mut publisher,
            heads.get_mut(&partition).unwrap(),
            1,
            0,
            series,
            1_000,
            1.0,
            &mut message,
            &mut prefix,
        );
        publisher
            .on_message_boundary(
                MessageSequence::new(1),
                completed(1, message, prefix),
                &mut heads,
                &mut labelsets,
            )
            .unwrap();

        let active_pending = publisher
            .pending
            .iter()
            .filter(|pending| !pending.handed_off)
            .collect::<Vec<_>>();
        assert_eq!(active_pending.len(), 1);
        let pending = active_pending[0];
        let wrong_identity = FrozenFragmentIdentity::for_fragment(
            LivePartitionKey::new("wrong-topic", 7),
            pending.fragment.as_ref(),
        )
        .unwrap();
        let mut wrong_builder = LiveSampleStoreBuilder::new();
        wrong_builder
            .insert_fragment(wrong_identity, Arc::clone(&pending.fragment))
            .unwrap();
        let wrong_store = wrong_builder.finish();
        let catalog = publisher.catalog.as_deref().unwrap();

        catalog.validate_sample_store(&wrong_store).unwrap();
        let failure = publisher
            .validate_active_owners(catalog, &wrong_store)
            .unwrap_err();
        assert_eq!(failure.class, PublicationFailureClass::TerminalIntegrity);
        assert!(
            failure.error.to_string().contains(
                "persistent sample root does not exactly match the supplied fragment identities"
            ),
            "unexpected error: {}",
            failure.error
        );
    }

    #[test]
    fn equal_numeric_partitions_in_two_topics_use_full_owner_validation() {
        let root = tempfile::tempdir().unwrap();
        let mut publisher = publisher(root.path(), Duration::from_nanos(1));
        let mut labelsets = LabelSetInterner::new_versioned_flat();
        let mut ingest_stats = OtlpMetricsIngestionStats::new();
        let first_series = labelsets
            .intern(
                &[
                    KeyValueRef::from((METRIC_NAME_LABEL, "live_metric")),
                    KeyValueRef::from(("host", "first")),
                ],
                &mut ingest_stats,
            )
            .unwrap();
        let second_series = labelsets
            .intern(
                &[
                    KeyValueRef::from((METRIC_NAME_LABEL, "live_metric")),
                    KeyValueRef::from(("host", "second")),
                ],
                &mut ingest_stats,
            )
            .unwrap();
        let first = PartitionKey::new("topic-a", 7);
        let second = PartitionKey::new("topic-b", 7);
        let mut heads = HashMap::from([
            (first.clone(), partition_head()),
            (second.clone(), partition_head()),
        ]);
        let mut prefix = CoverageLedger::empty();
        let mut message = CoverageLedger::empty();
        record_for_publication(
            &mut publisher,
            heads.get_mut(&first).unwrap(),
            1,
            0,
            first_series,
            1_000,
            1.0,
            &mut message,
            &mut prefix,
        );
        record_for_publication(
            &mut publisher,
            heads.get_mut(&second).unwrap(),
            1,
            1,
            second_series,
            2_000,
            2.0,
            &mut message,
            &mut prefix,
        );
        publisher
            .on_message_boundary(
                MessageSequence::new(1),
                completed(1, message, prefix),
                &mut heads,
                &mut labelsets,
            )
            .unwrap();

        let owner_stats = publisher
            .validate_active_owners(
                publisher.catalog.as_deref().unwrap(),
                &publisher.sample_store,
            )
            .unwrap();
        assert_eq!(owner_stats.active_partitions_capped, 2);
        assert!(!owner_stats.at_most_one_partition_fast_path);
        assert_eq!(owner_stats.run_keys_examined, 2);
        assert_eq!(owner_stats.id_buckets, 2);
        assert_eq!(owner_stats.canonical_identity_comparisons, 0);
    }

    #[test]
    fn owner_conflict_fails_closed_then_final_sealing_heals_it() {
        let root = tempfile::tempdir().unwrap();
        let mut publisher = publisher(root.path(), Duration::from_secs(60));
        let handle = publisher.handle();
        let (mut labelsets, series) = labelsets();
        let first = PartitionKey::new("topic-a", 7);
        let second = PartitionKey::new("topic-b", 7);
        let mut heads = HashMap::from([
            (first.clone(), partition_head()),
            (second.clone(), partition_head()),
        ]);
        let mut prefix = CoverageLedger::empty();
        let mut message = CoverageLedger::empty();
        record_for_publication(
            &mut publisher,
            heads.get_mut(&first).unwrap(),
            1,
            0,
            series,
            1_000,
            1.0,
            &mut message,
            &mut prefix,
        );
        record_for_publication(
            &mut publisher,
            heads.get_mut(&second).unwrap(),
            1,
            1,
            series,
            2_000,
            2.0,
            &mut message,
            &mut prefix,
        );

        assert!(
            publisher
                .on_message_boundary(
                    MessageSequence::new(1),
                    completed(1, message, prefix),
                    &mut heads,
                    &mut labelsets,
                )
                .is_err()
        );
        assert!(matches!(
            handle.pin(Instant::now()),
            Err(chronoxide_core::storage::live_view::LiveViewError::Failed(
                _
            ))
        ));

        publisher.shutdown(&mut heads, &mut labelsets).unwrap();
        let final_view = handle.pin(Instant::now()).unwrap();
        assert!(final_view.payload().head().is_empty());
        assert_eq!(query(&final_view), vec![(1_000, 1.0), (2_000, 2.0)]);
    }

    #[test]
    fn canonical_owner_conflict_detects_distinct_raw_refs_after_name_projection() {
        let root = tempfile::tempdir().unwrap();
        let mut publisher = publisher(root.path(), Duration::from_secs(60));
        let handle = publisher.handle();
        let mut labelsets = LabelSetInterner::new_versioned_flat();
        let mut stats = OtlpMetricsIngestionStats::new();
        let raw_name = "a.label";
        let projected_name = normalize_label_name(raw_name);
        let raw = labelsets
            .intern(
                &[
                    KeyValueRef::from((METRIC_NAME_LABEL, "live_metric")),
                    KeyValueRef::from((raw_name, "same-value")),
                ],
                &mut stats,
            )
            .unwrap();
        let projected = labelsets
            .intern(
                &[
                    KeyValueRef::from((METRIC_NAME_LABEL, "live_metric")),
                    KeyValueRef::from((projected_name.as_str(), "same-value")),
                ],
                &mut stats,
            )
            .unwrap();
        assert_ne!(raw, projected);

        let first = PartitionKey::new("topic-a", 7);
        let second = PartitionKey::new("topic-b", 7);
        let mut heads = HashMap::from([
            (first.clone(), partition_head()),
            (second.clone(), partition_head()),
        ]);
        let mut prefix = CoverageLedger::empty();
        let mut message = CoverageLedger::empty();
        record_for_publication(
            &mut publisher,
            heads.get_mut(&first).unwrap(),
            1,
            0,
            raw,
            1_000,
            1.0,
            &mut message,
            &mut prefix,
        );
        record_for_publication(
            &mut publisher,
            heads.get_mut(&second).unwrap(),
            1,
            1,
            projected,
            2_000,
            2.0,
            &mut message,
            &mut prefix,
        );

        let error = publisher
            .on_message_boundary(
                MessageSequence::new(1),
                completed(1, message, prefix),
                &mut heads,
                &mut labelsets,
            )
            .unwrap_err();
        assert!(error.to_string().contains("canonical live series"));
        assert!(matches!(
            handle.pin(Instant::now()),
            Err(chronoxide_core::storage::live_view::LiveViewError::Failed(
                _
            ))
        ));

        publisher.shutdown(&mut heads, &mut labelsets).unwrap();
        let final_view = handle.pin(Instant::now()).unwrap();
        assert!(final_view.payload().head().is_empty());
        assert_eq!(query(&final_view), vec![(1_000, 1.0), (2_000, 2.0)]);
    }

    #[test]
    fn eight_pinned_readers_keep_generation_while_writer_publishes_next() {
        let root = tempfile::tempdir().unwrap();
        let mut publisher = publisher(root.path(), Duration::from_nanos(1));
        let handle = publisher.handle();
        let (mut labelsets, series) = labelsets();
        let partition = PartitionKey::new("metrics", 0);
        let mut heads = HashMap::from([(partition, partition_head())]);
        let mut prefix = CoverageLedger::empty();
        let mut message = CoverageLedger::empty();
        record_for_publication(
            &mut publisher,
            heads.values_mut().next().unwrap(),
            1,
            0,
            series,
            1_000,
            1.0,
            &mut message,
            &mut prefix,
        );
        publisher
            .on_message_boundary(
                MessageSequence::new(1),
                completed(1, message, prefix),
                &mut heads,
                &mut labelsets,
            )
            .unwrap();

        let start = Arc::new(Barrier::new(9));
        let release = Arc::new(Barrier::new(9));
        let observed = Arc::new(Mutex::new(Vec::new()));
        let mut readers = Vec::new();
        for _ in 0..8 {
            let pinned = handle.pin(Instant::now()).unwrap();
            let start = Arc::clone(&start);
            let release = Arc::clone(&release);
            let observed = Arc::clone(&observed);
            readers.push(thread::spawn(move || {
                start.wait();
                release.wait();
                observed
                    .lock()
                    .unwrap()
                    .push((pinned.generation(), query(&pinned)));
            }));
        }
        start.wait();

        let mut message = CoverageLedger::empty();
        record_for_publication(
            &mut publisher,
            heads.values_mut().next().unwrap(),
            2,
            0,
            series,
            2_000,
            2.0,
            &mut message,
            &mut prefix,
        );
        publisher
            .on_message_boundary(
                MessageSequence::new(2),
                completed(2, message, prefix),
                &mut heads,
                &mut labelsets,
            )
            .unwrap();
        assert_eq!(
            query(&handle.pin(Instant::now()).unwrap()),
            vec![(1_000, 1.0), (2_000, 2.0)]
        );

        release.wait();
        for reader in readers {
            reader.join().unwrap();
        }
        let observed = observed.lock().unwrap();
        assert_eq!(observed.len(), 8);
        assert!(
            observed
                .iter()
                .all(|(generation, samples)| *generation == 1 && samples == &vec![(1_000, 1.0)])
        );
    }

    #[test]
    fn pinned_real_query_session_does_not_hold_the_ingestion_writer_during_publication() {
        let root = tempfile::tempdir().unwrap();
        let mut publisher = publisher(root.path(), Duration::from_nanos(1));
        let handle = publisher.handle();
        let (mut labelsets, series) = labelsets();
        let partition = PartitionKey::new("metrics", 0);
        let mut heads = HashMap::from([(partition, partition_head())]);
        let mut prefix = CoverageLedger::empty();

        let mut first = CoverageLedger::empty();
        record_for_publication(
            &mut publisher,
            heads.values_mut().next().unwrap(),
            1,
            0,
            series,
            1_000,
            1.0,
            &mut first,
            &mut prefix,
        );
        publisher
            .on_message_boundary(
                MessageSequence::new(1),
                completed(1, first, prefix),
                &mut heads,
                &mut labelsets,
            )
            .unwrap();

        let pinned = handle.pin(Instant::now()).unwrap();
        let (session_ready_tx, session_ready_rx) = std::sync::mpsc::channel();
        let release = Arc::new(Barrier::new(2));
        let query_release = Arc::clone(&release);
        let query_thread = thread::spawn(move || {
            let mut session = pinned
                .payload()
                .sealed()
                .query_session_with_head_view(pinned.payload().head())
                .unwrap();
            session_ready_tx.send(()).unwrap();
            query_release.wait();
            let results = session
                .query_selector(&SegmentSelector::metric("live_metric"), 0, 30_000)
                .unwrap();
            (
                pinned.generation(),
                results
                    .into_iter()
                    .flat_map(|result| result.samples)
                    .collect::<Vec<_>>(),
            )
        });
        session_ready_rx.recv().unwrap();

        let mut second = CoverageLedger::empty();
        record_for_publication(
            &mut publisher,
            heads.values_mut().next().unwrap(),
            2,
            0,
            series,
            2_000,
            2.0,
            &mut second,
            &mut prefix,
        );
        publisher
            .on_message_boundary(
                MessageSequence::new(2),
                completed(2, second, prefix),
                &mut heads,
                &mut labelsets,
            )
            .unwrap();
        let newest = handle.pin(Instant::now()).unwrap();
        assert_eq!(newest.generation(), 2);
        assert_eq!(query(&newest), vec![(1_000, 1.0), (2_000, 2.0)]);

        release.wait();
        assert_eq!(query_thread.join().unwrap(), (1, vec![(1_000, 1.0)]));
    }

    #[test]
    fn slow_reader_survives_several_publications_then_reclaims_obsolete_head_payload() {
        let root = tempfile::tempdir().unwrap();
        let mut publisher = publisher(root.path(), Duration::from_nanos(1));
        let handle = publisher.handle();
        let memory = publisher.memory_governor();
        let (mut labelsets, series) = labelsets();
        let partition = PartitionKey::new("metrics", 0);
        let mut heads = HashMap::from([(partition, partition_head())]);
        let mut prefix = CoverageLedger::empty();

        let mut first = CoverageLedger::empty();
        record_for_publication(
            &mut publisher,
            heads.values_mut().next().unwrap(),
            1,
            0,
            series,
            1_000,
            1.0,
            &mut first,
            &mut prefix,
        );
        publisher
            .on_message_boundary(
                MessageSequence::new(1),
                completed(1, first, prefix),
                &mut heads,
                &mut labelsets,
            )
            .unwrap();
        let slow = handle.pin(Instant::now()).unwrap();
        let slow_weak = Arc::downgrade(&slow);

        // The second message seals and retires generation 1's head supplier.
        // Two further publications make the reader deliberately old while it
        // continues to query its exact immutable generation.
        for (sequence, timestamp_ms, value) in
            [(2, 11_000, 11.0), (3, 12_000, 12.0), (4, 13_000, 13.0)]
        {
            let mut message = CoverageLedger::empty();
            record_for_publication(
                &mut publisher,
                heads.values_mut().next().unwrap(),
                sequence,
                0,
                series,
                timestamp_ms,
                value,
                &mut message,
                &mut prefix,
            );
            publisher
                .on_message_boundary(
                    MessageSequence::new(sequence),
                    completed(sequence, message, prefix),
                    &mut heads,
                    &mut labelsets,
                )
                .unwrap();
            assert_eq!(query(&slow), vec![(1_000, 1.0)]);
        }

        let current = handle.pin(Instant::now()).unwrap();
        assert_eq!(current.generation(), 4);
        assert_eq!(
            query(&current),
            vec![(1_000, 1.0), (11_000, 11.0), (12_000, 12.0), (13_000, 13.0),]
        );
        let charged_with_slow_reader = memory.stats().charged_bytes;
        drop(slow);
        assert!(
            slow_weak.upgrade().is_none(),
            "the obsolete generation must be reclaimed after its last reader"
        );
        assert!(
            memory.stats().charged_bytes < charged_with_slow_reader,
            "the retired generation's exclusive frozen-payload charge must be released"
        );
        assert_eq!(
            query(&current),
            vec![(1_000, 1.0), (11_000, 11.0), (12_000, 12.0), (13_000, 13.0),],
            "reclaiming an old generation must not invalidate the current root"
        );
    }

    #[test]
    fn manifest_handoff_pause_keeps_old_head_generation_queryable_until_one_swap() {
        let root = tempfile::tempdir().unwrap();
        let mut publisher = publisher(root.path(), Duration::from_nanos(1));
        let handle = publisher.handle();
        let (mut labelsets, series) = labelsets();
        let partition = PartitionKey::new("metrics", 0);
        let mut heads = HashMap::from([(partition, partition_head())]);
        let mut prefix = CoverageLedger::empty();

        let mut first = CoverageLedger::empty();
        record_for_publication(
            &mut publisher,
            heads.values_mut().next().unwrap(),
            1,
            0,
            series,
            1_000,
            1.0,
            &mut first,
            &mut prefix,
        );
        publisher
            .on_message_boundary(
                MessageSequence::new(1),
                completed(1, first, prefix),
                &mut heads,
                &mut labelsets,
            )
            .unwrap();

        let mut second = CoverageLedger::empty();
        record_for_publication(
            &mut publisher,
            heads.values_mut().next().unwrap(),
            2,
            0,
            series,
            11_000,
            11.0,
            &mut second,
            &mut prefix,
        );
        let (entered_tx, entered_rx) = std::sync::mpsc::channel();
        let release = Arc::new(Barrier::new(2));
        let release_for_hook = Arc::clone(&release);
        publisher.set_publication_hook(move |stage| {
            if stage == PublicationStage::ManifestHandoffCommitted {
                entered_tx.send(()).unwrap();
                release_for_hook.wait();
            }
        });

        let publisher_thread = thread::spawn(move || {
            publisher
                .on_message_boundary(
                    MessageSequence::new(2),
                    completed(2, second, prefix),
                    &mut heads,
                    &mut labelsets,
                )
                .unwrap();
        });
        entered_rx.recv().unwrap();

        let during_handoff = handle.pin(Instant::now()).unwrap();
        assert_eq!(during_handoff.generation(), 1);
        assert_eq!(query(&during_handoff), vec![(1_000, 1.0)]);

        release.wait();
        publisher_thread.join().unwrap();
        let after_swap = handle.pin(Instant::now()).unwrap();
        assert_eq!(after_swap.generation(), 2);
        assert_eq!(query(&after_swap), vec![(1_000, 1.0), (11_000, 11.0)]);
        assert!(matches!(
            after_swap.manifest_cut(),
            ManifestCut::Present { .. }
        ));
    }

    #[test]
    fn refresh_failure_rejects_new_pins_and_exact_retry_publishes_next_generation() {
        let root = tempfile::tempdir().unwrap();
        let mut publisher = publisher(root.path(), Duration::from_nanos(1));
        let handle = publisher.handle();
        let (mut labelsets, series) = labelsets();
        let partition = PartitionKey::new("metrics", 0);
        let mut heads = HashMap::from([(partition, partition_head())]);
        let mut prefix = CoverageLedger::empty();

        let mut first = CoverageLedger::empty();
        record_for_publication(
            &mut publisher,
            heads.values_mut().next().unwrap(),
            1,
            0,
            series,
            1_000,
            1.0,
            &mut first,
            &mut prefix,
        );
        publisher
            .on_message_boundary(
                MessageSequence::new(1),
                completed(1, first, prefix),
                &mut heads,
                &mut labelsets,
            )
            .unwrap();
        let pinned_before_failure = handle.pin(Instant::now()).unwrap();
        let readers_pinned = Arc::new(Barrier::new(5));
        let release_readers = Arc::new(Barrier::new(5));
        let (reader_tx, reader_rx) = std::sync::mpsc::channel();
        let mut readers = Vec::new();
        for _ in 0..4 {
            let handle = Arc::clone(&handle);
            let readers_pinned = Arc::clone(&readers_pinned);
            let release_readers = Arc::clone(&release_readers);
            let reader_tx = reader_tx.clone();
            readers.push(thread::spawn(move || {
                let pinned = handle.pin(Instant::now()).unwrap();
                readers_pinned.wait();
                release_readers.wait();
                reader_tx
                    .send((pinned.generation(), query(&pinned)))
                    .unwrap();
            }));
        }
        drop(reader_tx);
        readers_pinned.wait();

        let mut second = CoverageLedger::empty();
        record_for_publication(
            &mut publisher,
            heads.values_mut().next().unwrap(),
            2,
            0,
            series,
            11_000,
            11.0,
            &mut second,
            &mut prefix,
        );
        let manifest_dir = root.path().join("manifest");
        let injected = Arc::new(AtomicBool::new(false));
        let injected_for_hook = Arc::clone(&injected);
        let manifest_for_hook = manifest_dir.clone();
        publisher.set_publication_hook(move |stage| {
            if stage != PublicationStage::ManifestHandoffCommitted
                || injected_for_hook.swap(true, AtomicOrdering::AcqRel)
            {
                return;
            }
            let current = std::fs::read_to_string(manifest_for_hook.join("CURRENT")).unwrap();
            std::fs::OpenOptions::new()
                .append(true)
                .open(manifest_for_hook.join(current.trim()))
                .unwrap()
                .write_all(&[0x43])
                .unwrap();
        });

        assert!(
            publisher
                .on_message_boundary(
                    MessageSequence::new(2),
                    completed(2, second, prefix),
                    &mut heads,
                    &mut labelsets,
                )
                .is_err()
        );
        assert!(matches!(
            handle.pin(Instant::now()),
            Err(chronoxide_core::storage::live_view::LiveViewError::Failed(
                _
            ))
        ));
        assert_eq!(query(&pinned_before_failure), vec![(1_000, 1.0)]);
        assert_eq!(
            publisher.expected_unsealed.sample_count(),
            2,
            "manifest handoff must not retire exact ownership before root commit"
        );

        let current = std::fs::read_to_string(manifest_dir.join("CURRENT")).unwrap();
        let manifest_path = manifest_dir.join(current.trim());
        let length = std::fs::metadata(&manifest_path).unwrap().len();
        std::fs::OpenOptions::new()
            .write(true)
            .open(&manifest_path)
            .unwrap()
            .set_len(length - 1)
            .unwrap();

        publisher
            .on_message_boundary(
                MessageSequence::new(3),
                completed(3, CoverageLedger::empty(), prefix),
                &mut heads,
                &mut labelsets,
            )
            .unwrap();
        let recovered = handle.pin(Instant::now()).unwrap();
        assert_eq!(recovered.generation(), 2);
        assert_eq!(query(&recovered), vec![(1_000, 1.0), (11_000, 11.0)]);
        assert_eq!(query(&pinned_before_failure), vec![(1_000, 1.0)]);
        assert_eq!(
            publisher.expected_unsealed.sample_count(),
            1,
            "successful root commit retires only the handed-off exact order"
        );

        release_readers.wait();
        for reader in readers {
            reader.join().unwrap();
        }
        let observations = reader_rx.into_iter().collect::<Vec<_>>();
        assert_eq!(observations.len(), 4);
        assert!(
            observations
                .iter()
                .all(|(generation, samples)| *generation == 1 && samples == &vec![(1_000, 1.0)]),
            "all readers pinned before the refresh failure must finish on the coherent old root"
        );
    }

    #[test]
    fn memory_admission_failure_retains_fragment_and_fails_readiness() {
        let root = tempfile::tempdir().unwrap();
        let mut config = publisher_config(Duration::from_nanos(1));
        config.memory_admission_bytes = 1;
        let mut publisher =
            LivePublisher::new(config, publisher_writer_config(root.path())).unwrap();
        let handle = publisher.handle();
        let (mut labelsets, series) = labelsets();
        let partition = PartitionKey::new("metrics", 0);
        let mut heads = HashMap::from([(partition, partition_head())]);
        let mut prefix = CoverageLedger::empty();
        let mut message = CoverageLedger::empty();
        record_for_publication(
            &mut publisher,
            heads.values_mut().next().unwrap(),
            1,
            0,
            series,
            1_000,
            1.0,
            &mut message,
            &mut prefix,
        );

        assert!(
            publisher
                .on_message_boundary(
                    MessageSequence::new(1),
                    completed(1, message, prefix),
                    &mut heads,
                    &mut labelsets,
                )
                .is_err()
        );
        assert_eq!(publisher.pending.len(), 1);
        assert!(publisher.pending[0].memory_charge.is_none());
        assert!(matches!(
            handle.pin(Instant::now()),
            Err(chronoxide_core::storage::live_view::LiveViewError::Failed(
                _
            ))
        ));
    }

    #[test]
    fn retained_seal_attempt_excludes_later_same_range_fragment_and_keeps_kind_guard() {
        let root = tempfile::tempdir().unwrap();
        let mut publisher = publisher(root.path(), Duration::from_secs(60));
        let (labelsets, series) = labelsets();
        let partition = PartitionKey::new("metrics", 0);
        let mut heads = HashMap::from([(partition.clone(), partition_head())]);
        let mut prefix = CoverageLedger::empty();
        let manifest_dir = root.path().join("manifest");
        let manifest_name = manifest_file_name(1);
        std::fs::create_dir_all(&manifest_dir).unwrap();
        std::fs::write(manifest_dir.join(&manifest_name), []).unwrap();
        write_current(&manifest_dir, &manifest_name).unwrap();

        let mut first = CoverageLedger::empty();
        record(
            heads.get_mut(&partition).unwrap(),
            1,
            0,
            series,
            1_000,
            1.0,
            &mut first,
            &mut prefix,
        );
        publisher.freeze_heads(&mut heads).unwrap();
        publisher.retry_missing_memory_charges().unwrap();
        assert_eq!(publisher.pending.len(), 1);

        let group = SealGroupKey {
            start_ms: 0,
            end_ms: 10_000,
            partition: partition.clone(),
            payload_lane: SegmentPayloadLane::InOrder,
        };
        // CURRENT is valid, so prepare_append succeeds. Obstructing only its
        // temporary replacement then forces a real ambiguous failure after the
        // exact manifest record has been appended and synced.
        std::fs::create_dir(manifest_dir.join("CURRENT.tmp")).unwrap();
        let error = publisher.seal_group(&group, &labelsets).unwrap_err();
        assert!(
            error.to_string().contains("directory")
                || error.to_string().contains("Directory")
                || error.to_string().contains("Is a directory"),
            "{error}"
        );
        assert!(!publisher.pending[0].handed_off);
        assert!(
            publisher
                .seal_attempts
                .get(&group)
                .and_then(|attempt| attempt.writer.as_ref())
                .is_some_and(SegmentWriter::has_retryable_manifest_attempt)
        );

        // This fragment arrives after the logical attempt was retained but
        // belongs to the identical partition/range/lane.
        let mut second = CoverageLedger::empty();
        record(
            heads.get_mut(&partition).unwrap(),
            2,
            0,
            series,
            2_000,
            2.0,
            &mut second,
            &mut prefix,
        );
        publisher.freeze_heads(&mut heads).unwrap();
        publisher.retry_missing_memory_charges().unwrap();
        assert_eq!(publisher.pending.len(), 2);

        std::fs::remove_dir(manifest_dir.join("CURRENT.tmp")).unwrap();
        publisher.seal_group(&group, &labelsets).unwrap();
        assert_eq!(
            publisher
                .pending
                .iter()
                .map(|pending| pending.handed_off)
                .collect::<Vec<_>>(),
            vec![true, false]
        );
        assert_eq!(publisher.sealed_coverage.sample_count(), 1);

        let retirements = publisher.prepare_handed_off_retirements(&heads).unwrap();
        assert!(
            retirements.is_empty(),
            "the later same-key fragment still depends on its kind guard"
        );
        publisher.apply_handed_off_retirements(&mut heads, &retirements);
        let head = &mut heads.get_mut(&partition).unwrap().head;
        assert_eq!(head.kind_guard_count(), 1);

        let int_value = SampleValue::Int64(3);
        let contribution = RecordedSampleContribution::for_sample(
            RecordedSampleOrder::new(MessageSequence::new(3), 0),
            series,
            3_000,
            &int_value,
            &mut Vec::new(),
        )
        .unwrap();
        let _reserved = head.try_reserve_retained_window_for_publication().unwrap();
        let outcome = head
            .record_sample_with_coverage(series, 3_000, int_value, contribution)
            .unwrap();
        assert!(!outcome.recorded);
        assert!(outcome.completed_window.is_none());
        assert_eq!(head.kind_guard_count(), 1);
    }

    #[test]
    fn handoff_preparation_and_preflag_failure_leave_all_fragment_owners_atomic() {
        let root = tempfile::tempdir().unwrap();
        let mut publisher = publisher(root.path(), Duration::from_secs(60));
        let (labelsets, series) = labelsets();
        let partition = PartitionKey::new("metrics", 0);
        let mut heads = HashMap::from([(partition.clone(), partition_head())]);
        let mut message = CoverageLedger::empty();
        let mut prefix = CoverageLedger::empty();
        record(
            heads.get_mut(&partition).unwrap(),
            1,
            0,
            series,
            1_000,
            1.0,
            &mut message,
            &mut prefix,
        );
        publisher.freeze_heads(&mut heads).unwrap();
        publisher.retry_missing_memory_charges().unwrap();

        let identity = publisher
            .pending_fragment_identity(&publisher.pending[0])
            .unwrap();
        let sealed_before = publisher.sealed_coverage;
        let error = publisher
            .prepare_attempt_handoff(&[identity.clone(), identity])
            .unwrap_err();
        assert!(error.to_string().contains("lost or duplicated"));
        assert_eq!(publisher.sealed_coverage, sealed_before);
        assert!(publisher.pending.iter().all(|pending| !pending.handed_off));

        let group = SealGroupKey {
            start_ms: 0,
            end_ms: 10_000,
            partition,
            payload_lane: SegmentPayloadLane::InOrder,
        };
        publisher.overflow_next_handoff_coverage_merge = true;
        let error = publisher.seal_group(&group, &labelsets).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("merged recorded-sample coverage count overflows u64"),
            "{error}"
        );
        assert_eq!(publisher.sealed_coverage, sealed_before);
        assert!(publisher.pending.iter().all(|pending| !pending.handed_off));
        assert!(
            publisher
                .seal_attempts
                .get(&group)
                .is_some_and(|attempt| attempt.committed_outcome.is_some()),
            "checked handoff failure must retain the exact committed outcome"
        );

        // This second injection is deliberately later: after the real checked
        // merge succeeds but before any owner flag or the sealed ledger
        // changes.
        publisher.fail_next_preflag_handoff_commit = true;
        let error = publisher.seal_group(&group, &labelsets).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("injected pre-flag live handoff commit failure")
        );
        assert_eq!(publisher.sealed_coverage, sealed_before);
        assert!(publisher.pending.iter().all(|pending| !pending.handed_off));
        assert!(
            publisher
                .seal_attempts
                .get(&group)
                .is_some_and(|attempt| attempt.committed_outcome.is_some()),
            "the exact committed outcome must remain retryable"
        );

        publisher.seal_group(&group, &labelsets).unwrap();
        assert_eq!(publisher.sealed_coverage, message);
        assert!(publisher.pending.iter().all(|pending| pending.handed_off));
        assert!(!publisher.seal_attempts.contains_key(&group));
    }

    #[test]
    fn expected_run_reservation_reports_both_usize_capacity_boundaries_atomically() {
        let root = tempfile::tempdir().unwrap();
        let mut publisher = publisher(root.path(), Duration::from_secs(60));
        let expected_before = publisher.expected_unsealed.clone();

        publisher.reserved_expected_runs = usize::MAX;
        let error = publisher.reserve_expected_order_slot().unwrap_err();
        assert!(matches!(
            error.kind(),
            crate::error::ErrorKind::IoError(source)
                if source.kind() == io::ErrorKind::InvalidData
        ));
        assert_eq!(publisher.reserved_expected_runs, usize::MAX);
        assert_eq!(publisher.expected_unsealed, expected_before);

        publisher.reserved_expected_runs = usize::MAX - 1;
        let error = publisher.reserve_expected_order_slot().unwrap_err();
        assert!(matches!(
            error.kind(),
            crate::error::ErrorKind::IoError(source)
                if source.kind() == io::ErrorKind::OutOfMemory
        ));
        assert_eq!(publisher.reserved_expected_runs, usize::MAX - 1);
        assert_eq!(publisher.expected_unsealed, expected_before);
    }

    #[test]
    fn seal_groups_are_ordered_by_event_time_before_partition() {
        let early = SealGroupKey {
            start_ms: 0,
            end_ms: 10_000,
            partition: PartitionKey::new("z-topic", 9),
            payload_lane: SegmentPayloadLane::InOrder,
        };
        let late = SealGroupKey {
            start_ms: 10_000,
            end_ms: 20_000,
            partition: PartitionKey::new("a-topic", 0),
            payload_lane: SegmentPayloadLane::InOrder,
        };
        let mut groups = [late.clone(), early.clone()];
        groups.sort_unstable();
        assert_eq!(groups, [early, late]);
    }

    #[test]
    fn restart_rejects_missing_current_when_sealed_segments_survive() {
        let root = tempfile::tempdir().unwrap();
        let config = publisher_config(Duration::from_secs(1));
        let writer_config = publisher_writer_config(root.path());
        let mut writer = SegmentWriter::new(writer_config.clone()).unwrap();
        writer.record_sample(SeriesRef::new(1), 1_000, 1.0).unwrap();
        writer.flush().unwrap();
        std::fs::remove_file(root.path().join("manifest").join("CURRENT")).unwrap();

        let error = LivePublisher::new(config, writer_config)
            .err()
            .expect("live restart must not reinterpret a damaged store as empty");
        let io_error = match error.kind() {
            crate::error::ErrorKind::IoError(error) => error,
            other => panic!("expected I/O startup failure, got {other}"),
        };
        assert_eq!(io_error.kind(), io::ErrorKind::InvalidData);
        assert!(
            io_error.to_string().contains("manifestless segment path"),
            "{io_error}"
        );
    }
}
