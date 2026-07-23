use super::*;

/// Keep the standard `Vec` growth policy for small postings. Once a list owns
/// at least 64 KiB of references, add one midpoint capacity between adjacent
/// powers of two. This reduces large-list tail slack without a second pass
/// over every label.
const COMPACT_GROWTH_THRESHOLD_REFS: usize = 16_384;

#[derive(Debug, Default)]
pub(in crate::storage) struct CompactPostingsBuilder {
    postings: ExactPostingsIndex,
}

impl CompactPostingsBuilder {
    pub(in crate::storage) fn insert_monotonic(
        &mut self,
        label_name_sym: u32,
        label_value_sym: u32,
        series_ref: u32,
    ) -> io::Result<()> {
        let refs = self
            .postings
            .postings
            .entry((label_name_sym, label_value_sym))
            .or_default();
        match refs.last().copied() {
            Some(last) if last == series_ref => Ok(()),
            Some(last) if last < series_ref => {
                reserve_compact_growth(refs)?;
                refs.push(series_ref);
                Ok(())
            }
            _ => match refs.binary_search(&series_ref) {
                Ok(_) => Ok(()),
                Err(index) => {
                    reserve_compact_growth(refs)?;
                    refs.insert(index, series_ref);
                    Ok(())
                }
            },
        }
    }

    pub(in crate::storage) fn finish(self) -> ExactPostingsIndex {
        self.postings
    }
}

fn reserve_compact_growth(refs: &mut Vec<u32>) -> io::Result<()> {
    let capacity = refs.capacity();
    if refs.len() != capacity || capacity < COMPACT_GROWTH_THRESHOLD_REFS {
        return Ok(());
    }

    let midpoint_capacity = capacity
        .checked_add(capacity.div_ceil(2))
        .ok_or_else(capacity_overflow)?;
    let legacy_ceiling = capacity
        .checked_add(1)
        .and_then(usize::checked_next_power_of_two)
        .ok_or_else(capacity_overflow)?;
    let target_capacity = midpoint_capacity.min(legacy_ceiling);
    refs.try_reserve_exact(target_capacity - capacity)
        .map_err(|error| io::Error::new(io::ErrorKind::OutOfMemory, error))
}

fn capacity_overflow() -> io::Error {
    io::Error::new(
        io::ErrorKind::OutOfMemory,
        "compact postings capacity exceeds addressable memory",
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compact_builder_matches_legacy_for_monotonic_and_decreasing_refs() {
        let rows = [
            (1, 10, 4),
            (1, 10, 2),
            (1, 10, 2),
            (1, 10, 8),
            (2, 20, 3),
            (2, 20, 1),
        ];
        let mut candidate = CompactPostingsBuilder::default();
        let mut legacy = ExactPostingsIndex::default();
        for (name, value, series_ref) in rows {
            candidate.insert_monotonic(name, value, series_ref).unwrap();
            legacy.insert_monotonic(name, value, series_ref);
        }

        assert_eq!(candidate.finish(), legacy);
    }

    #[test]
    fn compact_growth_adds_bounded_midpoints_only_above_threshold() {
        let mut builder = CompactPostingsBuilder::default();
        for series_ref in 0..=COMPACT_GROWTH_THRESHOLD_REFS as u32 {
            builder.insert_monotonic(1, 10, series_ref).unwrap();
        }
        let refs = &builder.postings.postings[&(1, 10)];
        assert_eq!(refs.len(), COMPACT_GROWTH_THRESHOLD_REFS + 1);
        assert_eq!(
            refs.capacity(),
            COMPACT_GROWTH_THRESHOLD_REFS + COMPACT_GROWTH_THRESHOLD_REFS / 2
        );

        let second_midpoint = COMPACT_GROWTH_THRESHOLD_REFS * 3;
        for series_ref in (refs.len() as u32)..=second_midpoint as u32 {
            builder.insert_monotonic(1, 10, series_ref).unwrap();
        }
        let refs = &builder.postings.postings[&(1, 10)];
        assert_eq!(refs.len(), second_midpoint + 1);
        assert_eq!(refs.capacity(), COMPACT_GROWTH_THRESHOLD_REFS * 4);
        assert!(refs.capacity().is_power_of_two());
    }

    #[test]
    fn compact_capacity_never_exceeds_legacy_doubling() {
        let mut compact_capacity = COMPACT_GROWTH_THRESHOLD_REFS;
        for _ in 0..32 {
            let midpoint = compact_capacity
                .checked_add(compact_capacity.div_ceil(2))
                .unwrap();
            let legacy = (compact_capacity + 1).next_power_of_two();
            compact_capacity = midpoint.min(legacy);
            assert!(compact_capacity <= legacy);
        }
    }
}
