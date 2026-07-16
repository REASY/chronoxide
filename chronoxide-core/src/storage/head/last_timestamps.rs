use super::{SeriesRef, SeriesRefHashMap};

const PAGE_SHIFT: u32 = 12;
pub(super) const PAGE_LEN: usize = 1 << PAGE_SHIFT;
const PAGE_MASK: u32 = PAGE_LEN as u32 - 1;
pub(super) const DENSE_PAGE_THRESHOLD: usize = PAGE_LEN / 2;
const OCCUPANCY_WORD_BITS: usize = u64::BITS as usize;
const OCCUPANCY_WORDS: usize = PAGE_LEN / OCCUPANCY_WORD_BITS;

// Bound the flat page-state directory to 64 KiB on 64-bit targets. Higher refs are
// uncommon and remain safe in the sparse fallback.
const PAGED_REF_LIMIT: u32 = 1 << 24;
const MAX_PAGE_COUNT: usize = (PAGED_REF_LIMIT as usize) / PAGE_LEN;

struct TimestampPage {
    values: [u64; PAGE_LEN],
    occupied: [u64; OCCUPANCY_WORDS],
}

impl TimestampPage {
    fn new() -> Self {
        Self {
            values: [0; PAGE_LEN],
            occupied: [0; OCCUPANCY_WORDS],
        }
    }

    #[cfg(test)]
    fn get(&self, slot: usize) -> Option<u64> {
        let word = slot / OCCUPANCY_WORD_BITS;
        let mask = 1u64 << (slot % OCCUPANCY_WORD_BITS);
        (self.occupied[word] & mask != 0).then(|| self.values[slot])
    }

    fn get_mut(&mut self, slot: usize) -> Option<&mut u64> {
        let word = slot / OCCUPANCY_WORD_BITS;
        let mask = 1u64 << (slot % OCCUPANCY_WORD_BITS);
        (self.occupied[word] & mask != 0).then(|| &mut self.values[slot])
    }

    fn insert(&mut self, slot: usize, timestamp_ms: u64) -> Option<u64> {
        let word = slot / OCCUPANCY_WORD_BITS;
        let mask = 1u64 << (slot % OCCUPANCY_WORD_BITS);
        let previous = (self.occupied[word] & mask != 0).then(|| self.values[slot]);
        self.values[slot] = timestamp_ms;
        self.occupied[word] |= mask;
        previous
    }
}

enum TimestampPageState {
    Sparse { len: u16 },
    Dense(Box<TimestampPage>),
}

impl Default for TimestampPageState {
    fn default() -> Self {
        Self::Sparse { len: 0 }
    }
}

/// Adaptive storage for the latest accepted timestamp of each series.
///
/// `SeriesRef` values are globally dense, but each partition-local head may
/// observe only a strided subset. A page therefore starts in the sparse hash
/// fallback and becomes direct-addressed only once half of its slots are used,
/// where the dense representation is smaller as well as cheaper to access.
/// Refs above the bounded flat directory remain sparse.
#[derive(Default)]
pub(super) struct LastTimestampTable {
    pages: Vec<TimestampPageState>,
    sparse: SeriesRefHashMap<u64>,
    len: usize,
}

impl LastTimestampTable {
    #[cfg(test)]
    pub(super) fn get(&self, series: SeriesRef) -> Option<u64> {
        if let Some((page, slot)) = paged_slot(series)
            && let Some(TimestampPageState::Dense(values)) = self.pages.get(page)
        {
            return values.get(slot);
        }
        self.sparse.get(&series).copied()
    }

    pub(super) fn get_mut(&mut self, series: SeriesRef) -> Option<&mut u64> {
        if let Some((page, slot)) = paged_slot(series)
            && let Some(TimestampPageState::Dense(values)) = self.pages.get_mut(page)
        {
            return values.get_mut(slot);
        }
        self.sparse.get_mut(&series)
    }

    pub(super) fn insert(&mut self, series: SeriesRef, timestamp_ms: u64) -> Option<u64> {
        let Some((page_index, slot)) = paged_slot(series) else {
            let previous = self.sparse.insert(series, timestamp_ms);
            if previous.is_none() {
                self.len += 1;
            }
            return previous;
        };

        if self.pages.len() <= page_index {
            self.pages.resize_with(page_index + 1, Default::default);
        }
        if let TimestampPageState::Dense(page) = &mut self.pages[page_index] {
            let previous = page.insert(slot, timestamp_ms);
            if previous.is_none() {
                self.len += 1;
            }
            return previous;
        }

        let previous = self.sparse.insert(series, timestamp_ms);
        if previous.is_some() {
            return previous;
        }
        self.len += 1;

        let TimestampPageState::Sparse { len } = &mut self.pages[page_index] else {
            unreachable!("dense page returned above")
        };
        *len += 1;
        if usize::from(*len) == DENSE_PAGE_THRESHOLD {
            self.promote_page(page_index);
        }
        None
    }

    pub(super) fn is_empty(&self) -> bool {
        self.len == 0
    }

    fn promote_page(&mut self, page_index: usize) {
        let mut page = Box::new(TimestampPage::new());
        let first_ref = (page_index * PAGE_LEN) as u32;
        let mut promoted = 0;
        for slot in 0..PAGE_LEN {
            let series = SeriesRef::new(first_ref + slot as u32);
            if let Some(timestamp_ms) = self.sparse.remove(&series) {
                page.insert(slot, timestamp_ms);
                promoted += 1;
            }
        }
        debug_assert_eq!(promoted, DENSE_PAGE_THRESHOLD);
        self.pages[page_index] = TimestampPageState::Dense(page);
    }

    #[cfg(test)]
    pub(super) fn len(&self) -> usize {
        self.len
    }

    #[cfg(test)]
    pub(super) fn dense_page_count(&self) -> usize {
        self.pages
            .iter()
            .filter(|page| matches!(page, TimestampPageState::Dense(_)))
            .count()
    }

    #[cfg(test)]
    pub(super) fn sparse_len(&self) -> usize {
        self.sparse.len()
    }

    #[cfg(test)]
    pub(super) fn paged_allocated_bytes(&self) -> usize {
        self.pages
            .capacity()
            .saturating_mul(std::mem::size_of::<TimestampPageState>())
            .saturating_add(
                self.dense_page_count()
                    .saturating_mul(std::mem::size_of::<TimestampPage>()),
            )
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
