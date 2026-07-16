use crate::storage::metadata_governor::{MetadataCharge, MetadataUsageClass};
use crate::storage::metadata_runtime::SegmentGenerationProvenance;

use super::*;

/// Sorted-unique query-local series refs whose complete vector capacity is
/// charged to the store-wide in-flight metadata budget.
#[derive(Debug)]
pub(crate) struct GovernedSeriesRefSet {
    provenance: SegmentGenerationProvenance,
    series_count: u32,
    values: Vec<u32>,
    charge: MetadataCharge,
}

impl GovernedSeriesRefSet {
    pub(crate) fn len(&self) -> usize {
        self.values.len()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    pub(crate) fn charged_bytes(&self) -> u64 {
        self.charge.bytes()
    }

    pub(super) fn values(&self) -> &[u32] {
        &self.values
    }

    #[cfg(test)]
    pub(super) fn capacity_for_test(&self) -> usize {
        self.values.capacity()
    }
}

impl SegmentMetadataSession {
    pub(crate) fn series_ref_set(
        &self,
        root: &SegmentMetadataRoot,
        refs: &[u32],
    ) -> Result<GovernedSeriesRefSet, SegmentMetadataFacadeError> {
        self.ensure_root(root)?;
        for &series_ref in refs {
            validate_series_ref(root, series_ref)?;
        }
        self.build_ref_set(root, refs.len(), |values| {
            values.extend_from_slice(refs);
            values.sort_unstable();
            values.dedup();
        })
    }

    pub(crate) fn all_series_ref_set(
        &self,
        root: &SegmentMetadataRoot,
    ) -> Result<GovernedSeriesRefSet, SegmentMetadataFacadeError> {
        self.ensure_root(root)?;
        let count = usize::try_from(root.series_count)
            .map_err(|_| SegmentMetadataFacadeError::RefSetSizeOverflow)?;
        self.build_ref_set(root, count, |values| {
            values.extend(0..root.series_count);
        })
    }

    pub(crate) fn exact_postings_ref_set(
        &self,
        root: &SegmentMetadataRoot,
        postings: &SegmentExactPostings,
    ) -> Result<GovernedSeriesRefSet, SegmentMetadataFacadeError> {
        let refs = self.exact_postings_refs(root, postings)?;
        self.build_ref_set(root, refs.len(), |values| {
            values.extend_from_slice(refs);
        })
    }

    pub(crate) fn union_series_ref_sets(
        &self,
        root: &SegmentMetadataRoot,
        left: &GovernedSeriesRefSet,
        right: &GovernedSeriesRefSet,
    ) -> Result<GovernedSeriesRefSet, SegmentMetadataFacadeError> {
        self.ensure_sets(root, left, right)?;
        let upper = left
            .values
            .len()
            .checked_add(right.values.len())
            .ok_or(SegmentMetadataFacadeError::RefSetSizeOverflow)?
            .min(
                usize::try_from(root.series_count)
                    .map_err(|_| SegmentMetadataFacadeError::RefSetSizeOverflow)?,
            );
        self.build_ref_set(root, upper, |values| {
            merge_union(&left.values, &right.values, values);
        })
    }

    pub(crate) fn intersect_series_ref_sets(
        &self,
        root: &SegmentMetadataRoot,
        left: &GovernedSeriesRefSet,
        right: &GovernedSeriesRefSet,
    ) -> Result<GovernedSeriesRefSet, SegmentMetadataFacadeError> {
        self.ensure_sets(root, left, right)?;
        self.build_ref_set(root, left.values.len().min(right.values.len()), |values| {
            merge_intersection(&left.values, &right.values, values);
        })
    }

    pub(crate) fn difference_series_ref_sets(
        &self,
        root: &SegmentMetadataRoot,
        left: &GovernedSeriesRefSet,
        right: &GovernedSeriesRefSet,
    ) -> Result<GovernedSeriesRefSet, SegmentMetadataFacadeError> {
        self.ensure_sets(root, left, right)?;
        self.build_ref_set(root, left.values.len(), |values| {
            merge_difference(&left.values, &right.values, values);
        })
    }

    pub(crate) fn visit_series_ref_set(
        &self,
        root: &SegmentMetadataRoot,
        values: &GovernedSeriesRefSet,
        mut visitor: impl FnMut(u32) -> bool,
    ) -> Result<bool, SegmentMetadataFacadeError> {
        self.ensure_set(root, values)?;
        for series_ref in values.values.iter().copied() {
            if !visitor(series_ref) {
                return Ok(false);
            }
        }
        Ok(true)
    }

    fn build_ref_set(
        &self,
        root: &SegmentMetadataRoot,
        capacity: usize,
        fill: impl FnOnce(&mut Vec<u32>),
    ) -> Result<GovernedSeriesRefSet, SegmentMetadataFacadeError> {
        self.ensure_root(root)?;
        let declared = checked_ref_bytes(capacity)?;
        let governor = self
            .guard
            .reader(crate::storage::segment::SegmentFile::Indexes)?
            .runtime()
            .governor();
        let mut charge =
            governor.reserve_in_flight_for_usage(declared, MetadataUsageClass::Scratch)?;
        let mut values = Vec::new();
        values.try_reserve_exact(capacity).map_err(|error| {
            SegmentMetadataFacadeError::RefSetAllocation(io::Error::new(
                io::ErrorKind::OutOfMemory,
                format!("series-ref vector reservation failed: {error}"),
            ))
        })?;
        charge.reconcile(checked_ref_bytes(values.capacity())?)?;
        fill(&mut values);
        debug_assert!(values.windows(2).all(|window| window[0] < window[1]));
        debug_assert!(values.iter().all(|value| *value < root.series_count));
        Ok(GovernedSeriesRefSet {
            provenance: self.guard.provenance(),
            series_count: root.series_count,
            values,
            charge,
        })
    }

    fn ensure_sets(
        &self,
        root: &SegmentMetadataRoot,
        left: &GovernedSeriesRefSet,
        right: &GovernedSeriesRefSet,
    ) -> Result<(), SegmentMetadataFacadeError> {
        self.ensure_set(root, left)?;
        self.ensure_set(root, right)
    }

    pub(super) fn ensure_set(
        &self,
        root: &SegmentMetadataRoot,
        values: &GovernedSeriesRefSet,
    ) -> Result<(), SegmentMetadataFacadeError> {
        self.ensure_root(root)?;
        if !values.provenance.matches(&self.guard) {
            return Err(SegmentMetadataFacadeError::ForeignSegmentGeneration);
        }
        if values.series_count != root.series_count {
            return Err(SegmentMetadataFacadeError::ForeignLayoutBackend);
        }
        Ok(())
    }
}

fn validate_series_ref(
    root: &SegmentMetadataRoot,
    series_ref: u32,
) -> Result<(), SegmentMetadataFacadeError> {
    if series_ref < root.series_count {
        Ok(())
    } else {
        Err(SegmentMetadataFacadeError::InvalidSeriesRef {
            series_ref,
            series_count: root.series_count,
        })
    }
}

fn checked_ref_bytes(capacity: usize) -> Result<u64, SegmentMetadataFacadeError> {
    capacity
        .checked_mul(std::mem::size_of::<u32>())
        .and_then(|bytes| u64::try_from(bytes).ok())
        .ok_or(SegmentMetadataFacadeError::RefSetSizeOverflow)
}

fn merge_union(left: &[u32], right: &[u32], output: &mut Vec<u32>) {
    let mut left_index = 0usize;
    let mut right_index = 0usize;
    while left_index < left.len() && right_index < right.len() {
        match left[left_index].cmp(&right[right_index]) {
            std::cmp::Ordering::Less => {
                output.push(left[left_index]);
                left_index += 1;
            }
            std::cmp::Ordering::Equal => {
                output.push(left[left_index]);
                left_index += 1;
                right_index += 1;
            }
            std::cmp::Ordering::Greater => {
                output.push(right[right_index]);
                right_index += 1;
            }
        }
    }
    output.extend_from_slice(&left[left_index..]);
    output.extend_from_slice(&right[right_index..]);
}

fn merge_intersection(left: &[u32], right: &[u32], output: &mut Vec<u32>) {
    let mut left_index = 0usize;
    let mut right_index = 0usize;
    while left_index < left.len() && right_index < right.len() {
        match left[left_index].cmp(&right[right_index]) {
            std::cmp::Ordering::Less => left_index += 1,
            std::cmp::Ordering::Equal => {
                output.push(left[left_index]);
                left_index += 1;
                right_index += 1;
            }
            std::cmp::Ordering::Greater => right_index += 1,
        }
    }
}

fn merge_difference(left: &[u32], right: &[u32], output: &mut Vec<u32>) {
    let mut left_index = 0usize;
    let mut right_index = 0usize;
    while left_index < left.len() && right_index < right.len() {
        match left[left_index].cmp(&right[right_index]) {
            std::cmp::Ordering::Less => {
                output.push(left[left_index]);
                left_index += 1;
            }
            std::cmp::Ordering::Equal => {
                left_index += 1;
                right_index += 1;
            }
            std::cmp::Ordering::Greater => right_index += 1,
        }
    }
    output.extend_from_slice(&left[left_index..]);
}
