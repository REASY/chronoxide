use std::collections::hash_map;
#[cfg(test)]
use std::ops::Index;

use crate::labels::{SeriesRef, SeriesRefHashMap};

use super::HeadSeriesTableStats;
use iter::{AdaptiveIter, AdaptiveValues, AdaptiveValuesMut};
pub(in crate::storage::head) use iter::{IntoIter, Iter, Keys, Values, ValuesMut};

mod iter;
#[cfg(test)]
mod tests;

const PAGE_SHIFT: u32 = 12;
pub(super) const PAGE_LEN: usize = 1 << PAGE_SHIFT;
const PAGE_MASK: u32 = PAGE_LEN as u32 - 1;
pub(super) const DIRECT_PAGE_THRESHOLD: usize = 128;
const INVALID_VALUE_INDEX: u16 = u16::MAX;

// Bound the flat page-state directory to 4,096 entries. Higher refs remain
// safe in the sparse fallback instead of growing a mostly empty directory.
const PAGED_REF_LIMIT: u32 = 1 << 24;
const MAX_PAGE_COUNT: usize = (PAGED_REF_LIMIT as usize) / PAGE_LEN;

/// Per-window series storage with an optional adaptive direct-addressed path.
///
/// A partition-local head can observe globally dense `SeriesRef`s either as a
/// dense run or as a sparse stride. Adaptive pages therefore start in the
/// hash-map fallback and promote only after enough of their 4,096 slots are
/// occupied. The disabled representation is exactly the original hash map so
/// runtime A/B measurements do not carry the adaptive directory overhead.
#[derive(Debug)]
pub(super) enum HeadSeriesTable<V> {
    Plain {
        values: SeriesRefHashMap<V>,
        refs_above_paged_limit: usize,
    },
    Adaptive(AdaptiveSeriesTable<V>),
}

impl<V> Default for HeadSeriesTable<V> {
    fn default() -> Self {
        Self::new(false)
    }
}

impl<V> HeadSeriesTable<V> {
    pub(super) fn new(adaptive: bool) -> Self {
        if adaptive {
            Self::Adaptive(AdaptiveSeriesTable::default())
        } else {
            Self::Plain {
                values: SeriesRefHashMap::default(),
                refs_above_paged_limit: 0,
            }
        }
    }

    pub(super) fn len(&self) -> usize {
        match self {
            Self::Plain { values, .. } => values.len(),
            Self::Adaptive(series) => series.len,
        }
    }

    #[cfg(test)]
    pub(super) fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub(super) fn get(&self, series: SeriesRef) -> Option<&V> {
        match self {
            Self::Plain { values, .. } => values.get(&series),
            Self::Adaptive(values) => values.get(series),
        }
    }

    pub(super) fn get_mut(&mut self, series: SeriesRef) -> Option<&mut V> {
        match self {
            Self::Plain { values, .. } => values.get_mut(&series),
            Self::Adaptive(values) => values.get_mut(series),
        }
    }

    /// Inserts a series that is expected not to exist.
    ///
    /// On a duplicate, the table is unchanged and ownership of the proposed
    /// value is returned to the caller.
    pub(super) fn insert_new(&mut self, series: SeriesRef, value: V) -> Result<(), V> {
        match self {
            Self::Plain {
                values,
                refs_above_paged_limit,
            } => match values.entry(series) {
                hash_map::Entry::Vacant(entry) => {
                    entry.insert(value);
                    if series.get() >= PAGED_REF_LIMIT {
                        *refs_above_paged_limit += 1;
                    }
                    Ok(())
                }
                hash_map::Entry::Occupied(_) => Err(value),
            },
            Self::Adaptive(values) => values.insert_new(series, value),
        }
    }

    pub(super) fn values(&self) -> Values<'_, V> {
        match self {
            Self::Plain { values, .. } => Values::Plain(values.values()),
            Self::Adaptive(values) => Values::Adaptive(AdaptiveValues::new(values)),
        }
    }

    pub(super) fn values_mut(&mut self) -> ValuesMut<'_, V> {
        match self {
            Self::Plain { values, .. } => ValuesMut::Plain(values.values_mut()),
            Self::Adaptive(values) => ValuesMut::Adaptive(AdaptiveValuesMut::new(values)),
        }
    }

    pub(super) fn keys(&self) -> Keys<'_, V> {
        match self {
            Self::Plain { values, .. } => Keys::Plain(values.keys()),
            Self::Adaptive(values) => Keys::Adaptive(AdaptiveIter::new(values)),
        }
    }

    pub(super) fn iter(&self) -> Iter<'_, V> {
        match self {
            Self::Plain { values, .. } => Iter::Plain(values.iter()),
            Self::Adaptive(values) => Iter::Adaptive(AdaptiveIter::new(values)),
        }
    }

    pub(super) fn into_entries(self) -> IntoIter<V> {
        self.into_iter()
    }

    pub(super) fn stats(&self) -> HeadSeriesTableStats {
        match self {
            Self::Plain {
                values,
                refs_above_paged_limit,
            } => HeadSeriesTableStats {
                series: values.len(),
                sparse_series: values.len(),
                sparse_capacity: values.capacity(),
                refs_above_paged_limit: *refs_above_paged_limit,
                ..HeadSeriesTableStats::default()
            },
            Self::Adaptive(values) => values.stats(),
        }
    }

    /// Recomputes every maintained structural counter from the containers.
    ///
    /// This deliberately remains test-only: production telemetry must never
    /// turn a flush into an O(series + pages) scan.
    #[cfg(test)]
    fn scanned_stats(&self) -> HeadSeriesTableStats {
        match self {
            Self::Plain { values, .. } => HeadSeriesTableStats {
                series: values.len(),
                sparse_series: values.len(),
                sparse_capacity: values.capacity(),
                refs_above_paged_limit: values
                    .keys()
                    .filter(|series| series.get() >= PAGED_REF_LIMIT)
                    .count(),
                ..HeadSeriesTableStats::default()
            },
            Self::Adaptive(values) => values.scanned_stats(),
        }
    }

    #[cfg(test)]
    fn assert_stats_match_scan(&self) {
        assert_eq!(self.stats(), self.scanned_stats());
    }

    #[cfg(test)]
    pub(super) fn contains_key(&self, series: &SeriesRef) -> bool {
        self.get(*series).is_some()
    }

    #[cfg(test)]
    pub(super) fn remove(&mut self, series: &SeriesRef) -> Option<V> {
        match self {
            Self::Plain {
                values,
                refs_above_paged_limit,
            } => {
                let removed = values.remove(series);
                if removed.is_some() && series.get() >= PAGED_REF_LIMIT {
                    *refs_above_paged_limit -= 1;
                }
                removed
            }
            Self::Adaptive(values) => values.remove(*series),
        }
    }

    #[cfg(test)]
    pub(super) fn direct_page_count(&self) -> usize {
        match self {
            Self::Plain { .. } => 0,
            Self::Adaptive(values) => values
                .pages
                .iter()
                .filter(|page| matches!(page, SeriesPage::Direct(_)))
                .count(),
        }
    }

    #[cfg(test)]
    pub(super) fn sparse_len(&self) -> usize {
        match self {
            Self::Plain { values, .. } => values.len(),
            Self::Adaptive(values) => values.sparse.len(),
        }
    }

    #[cfg(test)]
    pub(super) fn page_directory_len(&self) -> usize {
        match self {
            Self::Plain { .. } => 0,
            Self::Adaptive(values) => values.pages.len(),
        }
    }
}

#[cfg(test)]
impl<V> Index<&SeriesRef> for HeadSeriesTable<V> {
    type Output = V;

    fn index(&self, series: &SeriesRef) -> &Self::Output {
        self.get(*series).expect("series is absent from head table")
    }
}

#[derive(Debug)]
pub(super) struct AdaptiveSeriesTable<V> {
    pages: Vec<SeriesPage<V>>,
    sparse: SeriesRefHashMap<V>,
    len: usize,
    sparse_pages: usize,
    refs_above_paged_limit: usize,
    sparse_slot_capacity: usize,
    direct_pages: usize,
    direct_series: usize,
    direct_reverse_slot_capacity: usize,
    direct_value_capacity: usize,
}

impl<V> Default for AdaptiveSeriesTable<V> {
    fn default() -> Self {
        Self {
            pages: Vec::new(),
            sparse: SeriesRefHashMap::default(),
            len: 0,
            sparse_pages: 0,
            refs_above_paged_limit: 0,
            sparse_slot_capacity: 0,
            direct_pages: 0,
            direct_series: 0,
            direct_reverse_slot_capacity: 0,
            direct_value_capacity: 0,
        }
    }
}

impl<V> AdaptiveSeriesTable<V> {
    fn stats(&self) -> HeadSeriesTableStats {
        HeadSeriesTableStats {
            adaptive: true,
            series: self.len,
            page_directory_len: self.pages.len(),
            page_directory_capacity: self.pages.capacity(),
            sparse_pages: self.sparse_pages,
            sparse_series: self.sparse.len(),
            sparse_capacity: self.sparse.capacity(),
            refs_above_paged_limit: self.refs_above_paged_limit,
            sparse_slot_capacity: self.sparse_slot_capacity,
            direct_pages: self.direct_pages,
            direct_series: self.direct_series,
            direct_slot_index_bytes: self
                .direct_pages
                .saturating_mul(PAGE_LEN)
                .saturating_mul(size_of::<u16>()),
            direct_reverse_slot_capacity: self.direct_reverse_slot_capacity,
            direct_value_capacity: self.direct_value_capacity,
        }
    }

    #[cfg(test)]
    fn scanned_stats(&self) -> HeadSeriesTableStats {
        let mut stats = HeadSeriesTableStats {
            adaptive: true,
            series: self.len,
            page_directory_len: self.pages.len(),
            page_directory_capacity: self.pages.capacity(),
            sparse_series: self.sparse.len(),
            sparse_capacity: self.sparse.capacity(),
            refs_above_paged_limit: self
                .sparse
                .keys()
                .filter(|series| series.get() >= PAGED_REF_LIMIT)
                .count(),
            ..HeadSeriesTableStats::default()
        };
        for page in &self.pages {
            match page {
                SeriesPage::Sparse { occupied_slots } => {
                    stats.sparse_pages += usize::from(!occupied_slots.is_empty());
                    stats.sparse_slot_capacity += occupied_slots.capacity();
                }
                SeriesPage::Direct(page) => {
                    stats.direct_pages += 1;
                    stats.direct_series += page.values.len();
                    stats.direct_slot_index_bytes += page.slot_indexes.len() * size_of::<u16>();
                    stats.direct_reverse_slot_capacity += page.reverse_slots.capacity();
                    stats.direct_value_capacity += page.values.capacity();
                }
            }
        }
        stats
    }

    fn get(&self, series: SeriesRef) -> Option<&V> {
        if let Some((page, slot)) = paged_slot(series)
            && let Some(SeriesPage::Direct(values)) = self.pages.get(page)
        {
            return values.get(slot);
        }
        self.sparse.get(&series)
    }

    fn get_mut(&mut self, series: SeriesRef) -> Option<&mut V> {
        if let Some((page, slot)) = paged_slot(series)
            && let Some(SeriesPage::Direct(values)) = self.pages.get_mut(page)
        {
            return values.get_mut(slot);
        }
        self.sparse.get_mut(&series)
    }

    fn insert_new(&mut self, series: SeriesRef, value: V) -> Result<(), V> {
        let Some((page_index, slot)) = paged_slot(series) else {
            let inserted = insert_sparse_new(&mut self.sparse, &mut self.len, series, value);
            if inserted.is_ok() {
                self.refs_above_paged_limit += 1;
            }
            return inserted;
        };

        if self.pages.len() <= page_index {
            self.pages.resize_with(page_index + 1, SeriesPage::default);
        }
        if let SeriesPage::Direct(page) = &mut self.pages[page_index] {
            let reverse_capacity_before = page.reverse_slots.capacity();
            let value_capacity_before = page.values.capacity();
            let result = page.insert_new(slot, value);
            if result.is_ok() {
                self.len += 1;
                self.direct_series += 1;
                self.direct_reverse_slot_capacity +=
                    page.reverse_slots.capacity() - reverse_capacity_before;
                self.direct_value_capacity += page.values.capacity() - value_capacity_before;
            }
            return result;
        }

        match self.sparse.entry(series) {
            hash_map::Entry::Occupied(_) => Err(value),
            hash_map::Entry::Vacant(entry) => {
                entry.insert(value);
                self.len += 1;

                let SeriesPage::Sparse { occupied_slots } = &mut self.pages[page_index] else {
                    unreachable!("direct page returned above")
                };
                if occupied_slots.is_empty() {
                    self.sparse_pages += 1;
                }
                let slot_capacity_before = occupied_slots.capacity();
                occupied_slots.push(slot as u16);
                self.sparse_slot_capacity += occupied_slots.capacity() - slot_capacity_before;
                if occupied_slots.len() == DIRECT_PAGE_THRESHOLD {
                    self.promote_page(page_index);
                }
                Ok(())
            }
        }
    }

    #[cfg(test)]
    fn remove(&mut self, series: SeriesRef) -> Option<V> {
        let Some((page_index, slot)) = paged_slot(series) else {
            let removed = self.sparse.remove(&series);
            if removed.is_some() {
                self.len -= 1;
                self.refs_above_paged_limit -= 1;
            }
            return removed;
        };

        match self.pages.get_mut(page_index) {
            Some(SeriesPage::Direct(page)) => {
                let removed = page.remove(slot);
                if removed.is_some() {
                    self.len -= 1;
                    self.direct_series -= 1;
                }
                removed
            }
            Some(SeriesPage::Sparse { occupied_slots }) => {
                let removed = self.sparse.remove(&series);
                if removed.is_some() {
                    let occupied_index = occupied_slots
                        .iter()
                        .position(|occupied| usize::from(*occupied) == slot)
                        .expect("sparse series must have a corresponding occupied slot");
                    occupied_slots.swap_remove(occupied_index);
                    if occupied_slots.is_empty() {
                        self.sparse_pages -= 1;
                    }
                    self.len -= 1;
                }
                removed
            }
            None => None,
        }
    }

    fn promote_page(&mut self, page_index: usize) {
        let SeriesPage::Sparse { occupied_slots } = std::mem::take(&mut self.pages[page_index])
        else {
            unreachable!("only sparse pages can be promoted")
        };

        self.sparse_pages -= 1;
        self.sparse_slot_capacity -= occupied_slots.capacity();
        let promoted_series = occupied_slots.len();
        let first_ref = (page_index * PAGE_LEN) as u32;
        let mut direct = DirectSeriesPage::with_capacity(occupied_slots.len());
        for slot in occupied_slots {
            let series = SeriesRef::new(first_ref + u32::from(slot));
            let value = self
                .sparse
                .remove(&series)
                .expect("sparse page slot must have a corresponding value");
            let result = direct.insert_new(usize::from(slot), value);
            debug_assert!(result.is_ok());
        }
        debug_assert_eq!(direct.values.len(), DIRECT_PAGE_THRESHOLD);
        self.direct_pages += 1;
        self.direct_series += promoted_series;
        self.direct_reverse_slot_capacity += direct.reverse_slots.capacity();
        self.direct_value_capacity += direct.values.capacity();
        self.pages[page_index] = SeriesPage::Direct(direct);
    }
}

fn insert_sparse_new<V>(
    sparse: &mut SeriesRefHashMap<V>,
    len: &mut usize,
    series: SeriesRef,
    value: V,
) -> Result<(), V> {
    match sparse.entry(series) {
        hash_map::Entry::Vacant(entry) => {
            entry.insert(value);
            *len += 1;
            Ok(())
        }
        hash_map::Entry::Occupied(_) => Err(value),
    }
}

#[derive(Debug)]
enum SeriesPage<V> {
    Sparse { occupied_slots: Vec<u16> },
    Direct(DirectSeriesPage<V>),
}

impl<V> Default for SeriesPage<V> {
    fn default() -> Self {
        Self::Sparse {
            occupied_slots: Vec::new(),
        }
    }
}

#[derive(Debug)]
struct DirectSeriesPage<V> {
    slot_indexes: Box<[u16]>,
    reverse_slots: Vec<u16>,
    values: Vec<V>,
}

impl<V> DirectSeriesPage<V> {
    fn with_capacity(capacity: usize) -> Self {
        Self {
            slot_indexes: vec![INVALID_VALUE_INDEX; PAGE_LEN].into_boxed_slice(),
            reverse_slots: Vec::with_capacity(capacity),
            values: Vec::with_capacity(capacity),
        }
    }

    fn get(&self, slot: usize) -> Option<&V> {
        let index = self.slot_indexes[slot];
        (index != INVALID_VALUE_INDEX).then(|| &self.values[usize::from(index)])
    }

    fn get_mut(&mut self, slot: usize) -> Option<&mut V> {
        let index = self.slot_indexes[slot];
        (index != INVALID_VALUE_INDEX).then(|| &mut self.values[usize::from(index)])
    }

    fn insert_new(&mut self, slot: usize, value: V) -> Result<(), V> {
        if self.slot_indexes[slot] != INVALID_VALUE_INDEX {
            return Err(value);
        }

        let index = self.values.len();
        debug_assert!(index < PAGE_LEN);
        self.slot_indexes[slot] = index as u16;
        self.reverse_slots.push(slot as u16);
        self.values.push(value);
        Ok(())
    }

    #[cfg(test)]
    fn remove(&mut self, slot: usize) -> Option<V> {
        let index = self.slot_indexes[slot];
        if index == INVALID_VALUE_INDEX {
            return None;
        }

        let index = usize::from(index);
        self.slot_indexes[slot] = INVALID_VALUE_INDEX;
        self.reverse_slots.swap_remove(index);
        let value = self.values.swap_remove(index);
        if index < self.values.len() {
            let moved_slot = usize::from(self.reverse_slots[index]);
            self.slot_indexes[moved_slot] = index as u16;
        }
        Some(value)
    }
}

fn paged_slot(series: SeriesRef) -> Option<(usize, usize)> {
    let raw = series.get();
    (raw < PAGED_REF_LIMIT).then(|| {
        let page = (raw >> PAGE_SHIFT) as usize;
        debug_assert!(page < MAX_PAGE_COUNT);
        (page, (raw & PAGE_MASK) as usize)
    })
}
