//! Exact live-view sequencing and successfully-recorded sample accounting.
//!
//! The coverage fingerprint is a diagnostic multiset accumulator. Structural
//! ownership remains the authority for deciding whether a sample is present
//! exactly once.

use std::fmt;
use std::io;

use sha2::{Digest, Sha256};
use smallvec::SmallVec;

use crate::labels::SeriesRef;

use super::encoding::VarLenEncoding;
use super::head::SampleValue;

const RECORDED_SAMPLE_DOMAIN: &[u8] = b"chronoxide-live-recorded-sample-v1\0";

/// A globally increasing ingestion-message sequence.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct MessageSequence(u64);

impl MessageSequence {
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u64 {
        self.0
    }
}

impl fmt::Display for MessageSequence {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

/// A checked, single-writer message sequence allocator.
///
/// `next()` permits `u64::MAX` exactly once and then remains exhausted. It
/// never wraps to zero.
#[derive(Debug, Clone)]
pub struct MessageSequencer {
    next: Option<u64>,
}

impl Default for MessageSequencer {
    fn default() -> Self {
        Self::starting_at(MessageSequence::new(1))
    }
}

impl MessageSequencer {
    pub const fn starting_at(next: MessageSequence) -> Self {
        Self {
            next: Some(next.get()),
        }
    }

    pub fn next_sequence(&mut self) -> io::Result<MessageSequence> {
        let value = self.next.ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "live message sequence exhausted u64 capacity",
            )
        })?;
        self.next = value.checked_add(1);
        Ok(MessageSequence::new(value))
    }

    pub const fn next_value(&self) -> Option<MessageSequence> {
        match self.next {
            Some(value) => Some(MessageSequence::new(value)),
            None => None,
        }
    }
}

/// Stable traversal order of one datapoint within one acquired message.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RecordedSampleOrder {
    message_sequence: MessageSequence,
    sample_ordinal: u64,
}

impl RecordedSampleOrder {
    pub const fn new(message_sequence: MessageSequence, sample_ordinal: u64) -> Self {
        Self {
            message_sequence,
            sample_ordinal,
        }
    }

    pub const fn message_sequence(self) -> MessageSequence {
        self.message_sequence
    }

    pub const fn sample_ordinal(self) -> u64 {
        self.sample_ordinal
    }
}

/// Checked ordinal allocator for one active message.
#[derive(Debug, Clone)]
pub struct MessageSampleOrdinals {
    sequence: MessageSequence,
    next: Option<u64>,
}

impl MessageSampleOrdinals {
    pub const fn new(sequence: MessageSequence) -> Self {
        Self {
            sequence,
            next: Some(0),
        }
    }

    pub fn next_order(&mut self) -> io::Result<RecordedSampleOrder> {
        let ordinal = self.next.ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "sample ordinal exhausted u64 capacity for message {}",
                    self.sequence
                ),
            )
        })?;
        self.next = ordinal.checked_add(1);
        Ok(RecordedSampleOrder::new(self.sequence, ordinal))
    }

    pub const fn sequence(&self) -> MessageSequence {
        self.sequence
    }
}

/// Inclusive first/last recorded order carried by one mutable/frozen fragment.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RecordedSampleOrderRange {
    first: RecordedSampleOrder,
    last: RecordedSampleOrder,
}

impl RecordedSampleOrderRange {
    pub const fn one(order: RecordedSampleOrder) -> Self {
        Self {
            first: order,
            last: order,
        }
    }

    pub const fn first(self) -> RecordedSampleOrder {
        self.first
    }

    pub const fn last(self) -> RecordedSampleOrder {
        self.last
    }

    pub fn checked_extend(self, order: RecordedSampleOrder) -> io::Result<Self> {
        if order <= self.last {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "recorded sample order must increase strictly: previous=({}, {}) next=({}, {})",
                    self.last.message_sequence().get(),
                    self.last.sample_ordinal(),
                    order.message_sequence().get(),
                    order.sample_ordinal(),
                ),
            ));
        }
        Ok(Self {
            first: self.first,
            last: order,
        })
    }
}

/// One canonical, inclusive run of adjacent successful ordinals in one message.
///
/// This is intentionally distinct from [`RecordedSampleOrderRange`]. A
/// fragment range is a coarse first/last span and may cross message
/// boundaries; an exact ownership run never may.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RecordedSampleOrderRun {
    first: RecordedSampleOrder,
    last: RecordedSampleOrder,
}

impl RecordedSampleOrderRun {
    const fn one(order: RecordedSampleOrder) -> Self {
        Self {
            first: order,
            last: order,
        }
    }

    pub const fn first(self) -> RecordedSampleOrder {
        self.first
    }

    pub const fn last(self) -> RecordedSampleOrder {
        self.last
    }

    pub fn sample_count(self) -> io::Result<u64> {
        if self.first.message_sequence != self.last.message_sequence {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "recorded-sample ownership run crosses a message boundary",
            ));
        }
        self.last
            .sample_ordinal
            .checked_sub(self.first.sample_ordinal)
            .and_then(|distance| distance.checked_add(1))
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "recorded-sample ownership run is reversed or overflows",
                )
            })
    }
}

/// Canonical exact membership of successfully recorded sample orders.
///
/// Adjacent ordinals from one message coalesce into one inclusive run.
/// Message boundaries and gaps remain explicit, so rejected datapoints and
/// samples routed to interleaved windows cannot be mistaken for ownership.
#[derive(Clone, Default, PartialEq, Eq)]
pub struct RecordedSampleOrderSet {
    runs: SmallVec<[RecordedSampleOrderRun; 2]>,
    sample_count: u64,
}

impl fmt::Debug for RecordedSampleOrderSet {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RecordedSampleOrderSet")
            .field("sample_count", &self.sample_count)
            .field("runs", &self.runs)
            .finish()
    }
}

#[derive(Debug)]
pub struct PreparedRecordedSampleAppend {
    order: RecordedSampleOrder,
    next_sample_count: u64,
    extend_last: bool,
    previous_last: Option<RecordedSampleOrder>,
}

impl RecordedSampleOrderSet {
    pub const fn empty() -> Self {
        Self {
            runs: SmallVec::new_const(),
            sample_count: 0,
        }
    }

    pub const fn sample_count(&self) -> u64 {
        self.sample_count
    }

    pub fn is_empty(&self) -> bool {
        self.runs.is_empty()
    }

    pub fn run_count(&self) -> usize {
        self.runs.len()
    }

    pub fn runs(&self) -> &[RecordedSampleOrderRun] {
        &self.runs
    }

    pub fn estimated_heap_bytes(&self) -> usize {
        if self.runs.spilled() {
            self.runs
                .capacity()
                .saturating_mul(std::mem::size_of::<RecordedSampleOrderRun>())
        } else {
            0
        }
    }

    /// Reserves capacity for at least `additional` runs beyond current exact
    /// membership without changing that membership.
    pub fn try_reserve_additional_runs(&mut self, additional: usize) -> io::Result<()> {
        self.runs.try_reserve(additional).map_err(|error| {
            io::Error::new(
                io::ErrorKind::OutOfMemory,
                format!("failed to reserve recorded-sample ownership runs: {error:?}"),
            )
        })
    }

    /// Reserves the worst-case additional run before a datapoint can mutate
    /// storage. Unused capacity after a rejected datapoint is harmless and is
    /// reused by later appends.
    pub fn try_reserve_additional_run(&mut self) -> io::Result<()> {
        self.try_reserve_additional_runs(1)
    }

    /// Validates and reserves an append without changing exact membership.
    ///
    /// Committing the returned token is allocation-free and infallible. This
    /// lets the head encode a sample between prepare and commit without an
    /// allocation failure creating data that is absent from the ownership
    /// proof.
    pub fn try_prepare_append(
        &mut self,
        order: RecordedSampleOrder,
    ) -> io::Result<PreparedRecordedSampleAppend> {
        let previous_last = self.runs.last().map(|run| run.last);
        if previous_last.is_some_and(|last| order <= last) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "recorded-sample ownership order must increase strictly",
            ));
        }
        let next_sample_count = self.sample_count.checked_add(1).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "recorded-sample ownership count overflows u64",
            )
        })?;
        let extend_last = previous_last.is_some_and(|last| {
            last.message_sequence == order.message_sequence
                && last
                    .sample_ordinal
                    .checked_add(1)
                    .is_some_and(|next| next == order.sample_ordinal)
        });
        if !extend_last {
            self.try_reserve_additional_run()?;
        }
        Ok(PreparedRecordedSampleAppend {
            order,
            next_sample_count,
            extend_last,
            previous_last,
        })
    }

    pub fn commit_prepared_append(&mut self, prepared: PreparedRecordedSampleAppend) {
        debug_assert_eq!(
            self.runs.last().map(|range| range.last),
            prepared.previous_last
        );
        if prepared.extend_last {
            let last = self
                .runs
                .last_mut()
                .expect("prepared ownership extension requires an existing run");
            last.last = prepared.order;
        } else {
            debug_assert!(self.runs.len() < self.runs.capacity());
            self.runs.push(RecordedSampleOrderRun::one(prepared.order));
        }
        self.sample_count = prepared.next_sample_count;
    }

    pub fn try_append_order(&mut self, order: RecordedSampleOrder) -> io::Result<()> {
        let prepared = self.try_prepare_append(order)?;
        self.commit_prepared_append(prepared);
        Ok(())
    }

    /// Appends a later message/order set using capacity reserved before the
    /// corresponding datapoints mutated the head.
    pub fn append_pre_reserved(&mut self, other: &Self) -> io::Result<()> {
        self.validate()?;
        other.validate()?;
        if other.is_empty() {
            return Ok(());
        }
        if self
            .runs
            .last()
            .zip(other.runs.first())
            .is_some_and(|(left, right)| left.last >= right.first)
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "recorded-sample ownership append overlaps or reorders existing membership",
            ));
        }
        let sample_count = self
            .sample_count
            .checked_add(other.sample_count)
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "recorded-sample ownership append count overflows u64",
                )
            })?;
        let merge_boundary = self
            .runs
            .last()
            .zip(other.runs.first())
            .is_some_and(|(left, right)| ranges_are_adjacent(*left, *right));
        let additional_runs = other.runs.len() - usize::from(merge_boundary);
        if self.runs.capacity().saturating_sub(self.runs.len()) < additional_runs {
            return Err(io::Error::new(
                io::ErrorKind::OutOfMemory,
                "recorded-sample ownership append was not reserved before sample mutation",
            ));
        }
        let append_from = if merge_boundary {
            self.runs
                .last_mut()
                .expect("adjacent ownership append requires an existing run")
                .last = other.runs[0].last;
            1
        } else {
            0
        };
        self.runs.extend_from_slice(&other.runs[append_from..]);
        self.sample_count = sample_count;
        debug_assert!(self.validate().is_ok());
        Ok(())
    }

    /// Returns the exact disjoint union. Any duplicate contribution fails.
    pub fn checked_union(&self, other: &Self) -> io::Result<Self> {
        self.validate()?;
        other.validate()?;
        let capacity = self
            .runs
            .len()
            .checked_add(other.runs.len())
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "recorded-sample ownership union run count overflows usize",
                )
            })?;
        let mut runs = SmallVec::<[RecordedSampleOrderRun; 2]>::new();
        runs.try_reserve(capacity).map_err(|error| {
            io::Error::new(
                io::ErrorKind::OutOfMemory,
                format!("failed to reserve ownership-union runs: {error:?}"),
            )
        })?;
        let mut left = self.runs.iter().copied().peekable();
        let mut right = other.runs.iter().copied().peekable();
        while left.peek().is_some() || right.peek().is_some() {
            let next = match (left.peek(), right.peek()) {
                (Some(left_range), Some(right_range)) if left_range.first <= right_range.first => {
                    left.next().expect("peeked left ownership run")
                }
                (Some(_), Some(_)) => right.next().expect("peeked right ownership run"),
                (Some(_), None) => left.next().expect("peeked left ownership run"),
                (None, Some(_)) => right.next().expect("peeked right ownership run"),
                (None, None) => unreachable!(),
            };
            push_disjoint_ownership_run(&mut runs, next)?;
        }
        let sample_count = self
            .sample_count
            .checked_add(other.sample_count)
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "recorded-sample ownership union count overflows u64",
                )
            })?;
        let merged = Self { runs, sample_count };
        merged.validate()?;
        Ok(merged)
    }

    pub fn validate(&self) -> io::Result<()> {
        let mut sample_count = 0u64;
        let mut previous: Option<RecordedSampleOrderRun> = None;
        for run in &self.runs {
            if let Some(previous) = previous {
                if run.first <= previous.last {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "recorded-sample ownership runs overlap or are reordered",
                    ));
                }
                if ranges_are_adjacent(previous, *run) {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "recorded-sample ownership contains non-canonical adjacent runs",
                    ));
                }
            }
            sample_count = sample_count
                .checked_add(run.sample_count()?)
                .ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        "recorded-sample ownership validated count overflows u64",
                    )
                })?;
            previous = Some(*run);
        }
        if sample_count != self.sample_count {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "recorded-sample ownership count disagrees with its exact runs",
            ));
        }
        Ok(())
    }
}

fn push_disjoint_ownership_run(
    runs: &mut SmallVec<[RecordedSampleOrderRun; 2]>,
    next: RecordedSampleOrderRun,
) -> io::Result<()> {
    let Some(previous) = runs.last_mut() else {
        runs.push(next);
        return Ok(());
    };
    if next.first <= previous.last {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "recorded-sample contribution has more than one structural owner",
        ));
    }
    if ranges_are_adjacent(*previous, next) {
        previous.last = next.last;
    } else {
        runs.push(next);
    }
    Ok(())
}

fn ranges_are_adjacent(left: RecordedSampleOrderRun, right: RecordedSampleOrderRun) -> bool {
    left.last.message_sequence == right.first.message_sequence
        && left
            .last
            .sample_ordinal
            .checked_add(1)
            .is_some_and(|next| next == right.first.sample_ordinal)
}

/// One 256-bit, order-independent aggregate value.
#[derive(Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct CoverageFingerprint([u8; 32]);

impl CoverageFingerprint {
    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    fn add_assign_mod_256(&mut self, other: Self) {
        let mut carry = 0u16;
        for (target, value) in self.0.iter_mut().zip(other.0) {
            let next = u16::from(*target) + u16::from(value) + carry;
            *target = next as u8;
            carry = next >> 8;
        }
    }
}

impl fmt::Debug for CoverageFingerprint {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "CoverageFingerprint(")?;
        for byte in self.0 {
            write!(formatter, "{byte:02x}")?;
        }
        write!(formatter, ")")
    }
}

/// Checked sample count plus a commutative 256-bit digest sum.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CoverageLedger {
    sample_count: u64,
    fingerprint: CoverageFingerprint,
}

impl CoverageLedger {
    pub const fn empty() -> Self {
        Self {
            sample_count: 0,
            fingerprint: CoverageFingerprint([0; 32]),
        }
    }

    pub const fn sample_count(self) -> u64 {
        self.sample_count
    }

    pub const fn fingerprint(self) -> CoverageFingerprint {
        self.fingerprint
    }

    pub fn checked_with_contribution(
        self,
        contribution: RecordedSampleContribution,
    ) -> io::Result<Self> {
        let sample_count = self.sample_count.checked_add(1).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "recorded-sample coverage count overflows u64",
            )
        })?;
        let mut fingerprint = self.fingerprint;
        fingerprint.add_assign_mod_256(contribution.fingerprint);
        Ok(Self {
            sample_count,
            fingerprint,
        })
    }

    pub fn checked_merge(self, other: Self) -> io::Result<Self> {
        let sample_count = self
            .sample_count
            .checked_add(other.sample_count)
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "merged recorded-sample coverage count overflows u64",
                )
            })?;
        let mut fingerprint = self.fingerprint;
        fingerprint.add_assign_mod_256(other.fingerprint);
        Ok(Self {
            sample_count,
            fingerprint,
        })
    }

    #[cfg(test)]
    pub(crate) const fn from_parts_for_test(
        sample_count: u64,
        fingerprint: CoverageFingerprint,
    ) -> Self {
        Self {
            sample_count,
            fingerprint,
        }
    }
}

/// The immutable coverage identity of one successfully recorded sample.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecordedSampleContribution {
    order: RecordedSampleOrder,
    fingerprint: CoverageFingerprint,
}

impl RecordedSampleContribution {
    pub fn for_sample(
        order: RecordedSampleOrder,
        series: SeriesRef,
        timestamp_ms: u64,
        value: &SampleValue,
        semantic_scratch: &mut Vec<u8>,
    ) -> io::Result<Self> {
        canonical_sample_semantic_bytes(value, semantic_scratch)?;
        let semantic_len = u64::try_from(semantic_scratch.len()).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "canonical sample semantic bytes exceed u64 capacity",
            )
        })?;

        let mut digest = Sha256::new();
        digest.update(RECORDED_SAMPLE_DOMAIN);
        digest.update(order.message_sequence().get().to_le_bytes());
        digest.update(order.sample_ordinal().to_le_bytes());
        digest.update(series.get().to_le_bytes());
        digest.update(timestamp_ms.to_le_bytes());
        digest.update([sample_kind_id(value)]);
        digest.update(semantic_len.to_le_bytes());
        digest.update(semantic_scratch.as_slice());
        Ok(Self {
            order,
            fingerprint: CoverageFingerprint(digest.finalize().into()),
        })
    }

    pub const fn order(self) -> RecordedSampleOrder {
        self.order
    }

    pub const fn ledger(self) -> CoverageLedger {
        CoverageLedger {
            sample_count: 1,
            fingerprint: self.fingerprint,
        }
    }
}

/// Writes the same value bytes used by topology-independent decoded-storage
/// fingerprinting: exact numeric bits and the canonical native typed codec.
pub fn canonical_sample_semantic_bytes(value: &SampleValue, out: &mut Vec<u8>) -> io::Result<()> {
    out.clear();
    match value {
        SampleValue::Float(value) => out.extend_from_slice(&value.to_bits().to_le_bytes()),
        SampleValue::Int64(value) => out.extend_from_slice(&value.to_le_bytes()),
        SampleValue::Histogram(value) => value.encode_into(out)?,
        SampleValue::ExponentialHistogram(value) => value.encode_into(out)?,
        SampleValue::Summary(value) => value.encode_into(out)?,
    }
    Ok(())
}

const fn sample_kind_id(value: &SampleValue) -> u8 {
    match value {
        SampleValue::Float(_) => 0,
        SampleValue::Int64(_) => 1,
        SampleValue::Histogram(_) => 2,
        SampleValue::ExponentialHistogram(_) => 3,
        SampleValue::Summary(_) => 4,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::thread;

    use super::*;
    use crate::storage::head::{
        CounterResetHint, ExponentialHistogramBuckets, ExponentialHistogramValue, HistogramValue,
        OtlpAggregationTemporality, SummaryQuantileValue, SummaryValue, TypedSampleMetadata,
    };

    fn order(message: u64, ordinal: u64) -> RecordedSampleOrder {
        RecordedSampleOrder::new(MessageSequence::new(message), ordinal)
    }

    fn contribution(
        message: u64,
        ordinal: u64,
        series: u32,
        timestamp_ms: u64,
        value: &SampleValue,
    ) -> RecordedSampleContribution {
        RecordedSampleContribution::for_sample(
            order(message, ordinal),
            SeriesRef::new(series),
            timestamp_ms,
            value,
            &mut Vec::new(),
        )
        .unwrap()
    }

    fn metadata() -> TypedSampleMetadata {
        TypedSampleMetadata {
            start_time_ms: Some(7),
            flags: 1,
            temporality: OtlpAggregationTemporality::Delta,
            reset_hint: CounterResetHint::CounterReset,
        }
    }

    fn all_typed_values() -> Vec<SampleValue> {
        vec![
            SampleValue::Float(f64::from_bits(0x7ff8_0000_0000_0042)),
            SampleValue::Int64(i64::MIN),
            SampleValue::Histogram(HistogramValue {
                count: 3,
                sum: Some(-0.0),
                min: Some(-1.0),
                max: Some(2.0),
                metadata: metadata(),
                explicit_bounds: vec![0.0],
                bucket_counts: vec![1, 2],
            }),
            SampleValue::ExponentialHistogram(ExponentialHistogramValue {
                count: 6,
                sum: Some(f64::INFINITY),
                min: None,
                max: Some(9.0),
                scale: -2,
                zero_threshold: -0.0,
                zero_count: 1,
                metadata: metadata(),
                positive: ExponentialHistogramBuckets {
                    offset: -3,
                    counts: vec![2],
                },
                negative: ExponentialHistogramBuckets {
                    offset: 4,
                    counts: vec![3],
                },
            }),
            SampleValue::Summary(SummaryValue {
                count: 2,
                sum: -0.0,
                metadata: metadata(),
                quantiles: vec![SummaryQuantileValue {
                    quantile: 0.5,
                    value: f64::NEG_INFINITY,
                }],
            }),
        ]
    }

    #[test]
    fn sequence_and_ordinal_allocators_reach_max_once_without_wrapping() {
        let mut messages = MessageSequencer::starting_at(MessageSequence::new(u64::MAX));
        assert_eq!(messages.next_sequence().unwrap().get(), u64::MAX);
        assert!(messages.next_sequence().is_err());
        assert!(messages.next_value().is_none());

        let mut ordinals = MessageSampleOrdinals {
            sequence: MessageSequence::new(9),
            next: Some(u64::MAX),
        };
        assert_eq!(ordinals.next_order().unwrap().sample_ordinal(), u64::MAX);
        assert!(ordinals.next_order().is_err());
    }

    #[test]
    fn coarse_order_range_requires_strict_traversal_but_may_cross_messages() {
        let range = RecordedSampleOrderRange::one(order(2, 3))
            .checked_extend(order(2, 8))
            .unwrap()
            .checked_extend(order(3, 0))
            .unwrap();
        assert_eq!(range.first(), order(2, 3));
        assert_eq!(range.last(), order(3, 0));
        assert!(range.checked_extend(order(3, 0)).is_err());
        assert!(range.checked_extend(order(2, 9)).is_err());
    }

    #[test]
    fn exact_order_set_preserves_sparse_ordinals_and_message_boundaries() {
        let mut exact = RecordedSampleOrderSet::empty();
        for order in [order(2, 0), order(2, 1), order(2, 3), order(3, 0)] {
            exact.try_append_order(order).unwrap();
        }

        exact.validate().unwrap();
        assert_eq!(exact.sample_count(), 4);
        assert_eq!(exact.run_count(), 3);
        assert_eq!(exact.runs()[0].first(), order(2, 0));
        assert_eq!(exact.runs()[0].last(), order(2, 1));
        assert_eq!(exact.runs()[1].first(), order(2, 3));
        assert_eq!(exact.runs()[1].last(), order(2, 3));
        assert_eq!(exact.runs()[2].first(), order(3, 0));
        assert_eq!(exact.runs()[2].last(), order(3, 0));
    }

    #[test]
    fn exact_order_append_rejects_duplicate_and_reordered_membership_transactionally() {
        let mut exact = RecordedSampleOrderSet::empty();
        exact.try_append_order(order(7, 2)).unwrap();
        let baseline = exact.clone();

        assert!(exact.try_append_order(order(7, 2)).is_err());
        assert_eq!(exact, baseline);
        assert!(exact.try_append_order(order(6, 99)).is_err());
        assert_eq!(exact, baseline);
    }

    #[test]
    fn exact_disjoint_union_canonicalizes_adjacency_and_rejects_overlap() {
        let mut left = RecordedSampleOrderSet::empty();
        left.try_append_order(order(1, 0)).unwrap();
        left.try_append_order(order(1, 2)).unwrap();
        let mut right = RecordedSampleOrderSet::empty();
        right.try_append_order(order(1, 1)).unwrap();
        right.try_append_order(order(2, 0)).unwrap();

        let union = left.checked_union(&right).unwrap();
        assert_eq!(union.sample_count(), 4);
        assert_eq!(union.run_count(), 2);
        assert_eq!(union.runs()[0].first(), order(1, 0));
        assert_eq!(union.runs()[0].last(), order(1, 2));
        assert_eq!(union.runs()[1].first(), order(2, 0));

        let mut duplicate = RecordedSampleOrderSet::empty();
        duplicate.try_append_order(order(1, 2)).unwrap();
        assert!(left.checked_union(&duplicate).is_err());
    }

    #[test]
    fn pre_reserved_append_failure_preserves_both_exact_sets() {
        let mut expected = RecordedSampleOrderSet::empty();
        expected.try_append_order(order(1, 0)).unwrap();
        expected.try_append_order(order(2, 0)).unwrap();
        let mut completed = RecordedSampleOrderSet::empty();
        completed.try_append_order(order(3, 0)).unwrap();
        let expected_before = expected.clone();
        let completed_before = completed.clone();

        assert!(expected.append_pre_reserved(&completed).is_err());
        assert_eq!(expected, expected_before);
        assert_eq!(completed, completed_before);

        expected.try_reserve_additional_runs(1).unwrap();
        expected.append_pre_reserved(&completed).unwrap();
        assert_eq!(expected.sample_count(), 3);
        assert_eq!(expected.run_count(), 3);
    }

    #[test]
    fn pre_reserved_append_keeps_adjacent_same_message_runs_canonical() {
        let mut expected = RecordedSampleOrderSet::empty();
        expected.try_append_order(order(4, 0)).unwrap();
        let mut completed_part = RecordedSampleOrderSet::empty();
        completed_part.try_append_order(order(4, 1)).unwrap();

        expected.append_pre_reserved(&completed_part).unwrap();
        expected.validate().unwrap();
        assert_eq!(expected.sample_count(), 2);
        assert_eq!(expected.run_count(), 1);
        assert_eq!(expected.runs()[0].last(), order(4, 1));
    }

    #[test]
    fn canonical_semantics_preserve_exact_numeric_and_all_native_typed_fields() {
        let mut scratch = Vec::new();
        for value in all_typed_values() {
            canonical_sample_semantic_bytes(&value, &mut scratch).unwrap();
            let expected = match &value {
                SampleValue::Float(value) => value.to_bits().to_le_bytes().to_vec(),
                SampleValue::Int64(value) => value.to_le_bytes().to_vec(),
                SampleValue::Histogram(value) => {
                    let mut encoded = Vec::new();
                    value.encode_into(&mut encoded).unwrap();
                    encoded
                }
                SampleValue::ExponentialHistogram(value) => {
                    let mut encoded = Vec::new();
                    value.encode_into(&mut encoded).unwrap();
                    encoded
                }
                SampleValue::Summary(value) => {
                    let mut encoded = Vec::new();
                    value.encode_into(&mut encoded).unwrap();
                    encoded
                }
            };
            assert_eq!(scratch, expected);
            let baseline = contribution(1, 0, 7, 99, &value);
            let changed_order = contribution(1, 1, 7, 99, &value);
            let changed_series = contribution(1, 0, 8, 99, &value);
            let changed_time = contribution(1, 0, 7, 100, &value);
            assert_ne!(baseline, changed_order);
            assert_ne!(baseline, changed_series);
            assert_ne!(baseline, changed_time);
        }

        assert_ne!(
            contribution(1, 0, 1, 1, &SampleValue::Float(0.0)),
            contribution(1, 0, 1, 1, &SampleValue::Float(-0.0))
        );
    }

    #[test]
    fn aggregate_is_order_independent_and_retains_multiplicity() {
        let values = all_typed_values();
        let contributions = values
            .iter()
            .enumerate()
            .map(|(index, value)| contribution(1, index as u64, 4, 10 + index as u64, value))
            .collect::<Vec<_>>();
        let left = contributions
            .iter()
            .try_fold(CoverageLedger::empty(), |ledger, value| {
                ledger.checked_with_contribution(*value)
            })
            .unwrap();
        let right = contributions
            .iter()
            .rev()
            .try_fold(CoverageLedger::empty(), |ledger, value| {
                ledger.checked_with_contribution(*value)
            })
            .unwrap();
        assert_eq!(left, right);

        let duplicate = left.checked_with_contribution(contributions[0]).unwrap();
        assert_eq!(duplicate.sample_count(), left.sample_count() + 1);
        assert_ne!(duplicate.fingerprint(), left.fingerprint());
    }

    #[test]
    fn checked_counts_fail_without_mutating_the_input() {
        let full =
            CoverageLedger::from_parts_for_test(u64::MAX, CoverageFingerprint::from_bytes([7; 32]));
        let original = full;
        assert!(
            full.checked_with_contribution(contribution(1, 0, 0, 0, &SampleValue::Int64(0)))
                .is_err()
        );
        assert_eq!(full, original);
        assert!(
            full.checked_merge(
                CoverageLedger::empty()
                    .checked_merge(CoverageLedger::from_parts_for_test(
                        1,
                        CoverageFingerprint::default()
                    ))
                    .unwrap()
            )
            .is_err()
        );
    }

    #[test]
    fn independently_built_thread_ledgers_merge_deterministically() {
        let values = Arc::new(all_typed_values());
        let handles = (0..8u64)
            .map(|message| {
                let values = Arc::clone(&values);
                thread::spawn(move || {
                    values
                        .iter()
                        .enumerate()
                        .try_fold(CoverageLedger::empty(), |ledger, (ordinal, value)| {
                            ledger.checked_with_contribution(contribution(
                                message + 1,
                                ordinal as u64,
                                message as u32,
                                ordinal as u64,
                                value,
                            ))
                        })
                        .unwrap()
                })
            })
            .collect::<Vec<_>>();
        let mut parts = handles
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .collect::<Vec<_>>();
        let forward = parts
            .iter()
            .try_fold(CoverageLedger::empty(), |all, part| {
                all.checked_merge(*part)
            })
            .unwrap();
        parts.reverse();
        let reverse = parts
            .iter()
            .try_fold(CoverageLedger::empty(), |all, part| {
                all.checked_merge(*part)
            })
            .unwrap();
        assert_eq!(forward, reverse);
        assert_eq!(forward.sample_count(), 8 * values.len() as u64);
    }

    #[test]
    fn independently_built_thread_order_sets_have_one_deterministic_exact_union() {
        let handles = (0..8u64)
            .map(|message| {
                thread::spawn(move || {
                    let mut exact = RecordedSampleOrderSet::empty();
                    for ordinal in [0, 2, 3, 7] {
                        exact.try_append_order(order(message + 1, ordinal)).unwrap();
                    }
                    exact
                })
            })
            .collect::<Vec<_>>();
        let mut parts = handles
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .collect::<Vec<_>>();
        let forward = parts
            .iter()
            .try_fold(RecordedSampleOrderSet::empty(), |all, part| {
                all.checked_union(part)
            })
            .unwrap();
        parts.reverse();
        let reverse = parts
            .iter()
            .try_fold(RecordedSampleOrderSet::empty(), |all, part| {
                all.checked_union(part)
            })
            .unwrap();

        assert_eq!(forward, reverse);
        assert_eq!(forward.sample_count(), 32);
        assert_eq!(forward.run_count(), 24);
    }
}
