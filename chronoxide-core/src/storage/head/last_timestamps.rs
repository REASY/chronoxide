use super::{LastTimestampTableStats, SeriesRef, SeriesRefHashMap};

const PAGE_SHIFT: u32 = 12;
pub(super) const PAGE_LEN: usize = 1 << PAGE_SHIFT;
const PAGE_MASK: u32 = PAGE_LEN as u32 - 1;
pub(super) const DENSE_PAGE_THRESHOLD: usize = PAGE_LEN / 2;
const OCCUPANCY_WORD_BITS: usize = u64::BITS as usize;
const OCCUPANCY_WORDS: usize = PAGE_LEN / OCCUPANCY_WORD_BITS;

// Bound the flat page-state directory to 64 KiB on 64-bit targets. Higher refs are
// uncommon and remain safe in the sparse fallback.
pub(super) const PAGED_REF_LIMIT: u32 = 1 << 24;
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

    #[cfg(test)]
    fn occupied_len(&self) -> usize {
        self.occupied
            .iter()
            .map(|word| word.count_ones() as usize)
            .sum()
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
pub(super) struct AdaptiveLastTimestampTable {
    pages: Vec<TimestampPageState>,
    sparse: SeriesRefHashMap<u64>,
    len: usize,
    sparse_pages: usize,
    dense_pages: usize,
    dense_series: usize,
    refs_above_paged_limit: usize,
}

impl AdaptiveLastTimestampTable {
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
                self.refs_above_paged_limit += 1;
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
                self.dense_series += 1;
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
        if *len == 0 {
            self.sparse_pages += 1;
        }
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
        self.sparse_pages -= 1;
        self.dense_pages += 1;
        self.dense_series += promoted;
    }
}

#[derive(Default)]
pub(super) struct PlainLastTimestampTable {
    values: SeriesRefHashMap<u64>,
    refs_above_paged_limit: usize,
}

impl PlainLastTimestampTable {
    fn insert(&mut self, series: SeriesRef, timestamp_ms: u64) -> Option<u64> {
        let previous = self.values.insert(series, timestamp_ms);
        if previous.is_none() && series.get() >= PAGED_REF_LIMIT {
            self.refs_above_paged_limit += 1;
        }
        previous
    }
}

/// Runtime-selectable last-timestamp representation used for same-binary A/Bs.
pub(super) enum LastTimestampTable {
    Plain(PlainLastTimestampTable),
    Adaptive(AdaptiveLastTimestampTable),
}

impl Default for LastTimestampTable {
    fn default() -> Self {
        Self::new(true)
    }
}

impl LastTimestampTable {
    pub(super) fn new(adaptive: bool) -> Self {
        if adaptive {
            Self::Adaptive(AdaptiveLastTimestampTable::default())
        } else {
            Self::Plain(PlainLastTimestampTable::default())
        }
    }

    #[cfg(test)]
    pub(super) fn get(&self, series: SeriesRef) -> Option<u64> {
        match self {
            Self::Plain(values) => values.values.get(&series).copied(),
            Self::Adaptive(values) => values.get(series),
        }
    }

    pub(super) fn get_mut(&mut self, series: SeriesRef) -> Option<&mut u64> {
        match self {
            Self::Plain(values) => values.values.get_mut(&series),
            Self::Adaptive(values) => values.get_mut(series),
        }
    }

    pub(super) fn insert(&mut self, series: SeriesRef, timestamp_ms: u64) -> Option<u64> {
        match self {
            Self::Plain(values) => values.insert(series, timestamp_ms),
            Self::Adaptive(values) => values.insert(series, timestamp_ms),
        }
    }

    pub(super) fn is_empty(&self) -> bool {
        match self {
            Self::Plain(values) => values.values.is_empty(),
            Self::Adaptive(values) => values.is_empty(),
        }
    }

    pub(super) fn stats(&self) -> LastTimestampTableStats {
        match self {
            Self::Plain(values) => LastTimestampTableStats {
                adaptive: false,
                series: values.values.len(),
                sparse_series: values.values.len(),
                sparse_capacity: values.values.capacity(),
                refs_above_paged_limit: values.refs_above_paged_limit,
                ..LastTimestampTableStats::default()
            },
            Self::Adaptive(values) => LastTimestampTableStats {
                adaptive: true,
                series: values.len,
                page_directory_len: values.pages.len(),
                page_directory_capacity: values.pages.capacity(),
                sparse_pages: values.sparse_pages,
                sparse_series: values.sparse.len(),
                sparse_capacity: values.sparse.capacity(),
                refs_above_paged_limit: values.refs_above_paged_limit,
                dense_pages: values.dense_pages,
                dense_series: values.dense_series,
                paged_allocated_bytes: values
                    .pages
                    .capacity()
                    .saturating_mul(std::mem::size_of::<TimestampPageState>())
                    .saturating_add(
                        values
                            .dense_pages
                            .saturating_mul(std::mem::size_of::<TimestampPage>()),
                    ),
            },
        }
    }

    #[cfg(test)]
    pub(super) fn assert_stats_counters(&self) {
        match self {
            Self::Plain(values) => {
                assert_eq!(
                    values.refs_above_paged_limit,
                    values
                        .values
                        .keys()
                        .filter(|series| series.get() >= PAGED_REF_LIMIT)
                        .count()
                );
            }
            Self::Adaptive(values) => {
                let mut sparse_pages = 0;
                let mut dense_pages = 0;
                let mut dense_series = 0;
                for page in &values.pages {
                    match page {
                        TimestampPageState::Sparse { len } => {
                            sparse_pages += usize::from(*len != 0);
                        }
                        TimestampPageState::Dense(page) => {
                            dense_pages += 1;
                            dense_series += page.occupied_len();
                        }
                    }
                }
                assert_eq!(values.sparse_pages, sparse_pages);
                assert_eq!(values.dense_pages, dense_pages);
                assert_eq!(values.dense_series, dense_series);
                assert_eq!(values.len, values.sparse.len() + dense_series);
                assert_eq!(
                    values.refs_above_paged_limit,
                    values
                        .sparse
                        .keys()
                        .filter(|series| series.get() >= PAGED_REF_LIMIT)
                        .count()
                );
            }
        }
    }

    #[cfg(test)]
    pub(super) fn len(&self) -> usize {
        self.stats().series
    }

    #[cfg(test)]
    pub(super) fn dense_page_count(&self) -> usize {
        self.stats().dense_pages
    }

    #[cfg(test)]
    pub(super) fn sparse_len(&self) -> usize {
        self.stats().sparse_series
    }

    #[cfg(test)]
    pub(super) fn paged_allocated_bytes(&self) -> usize {
        self.stats().paged_allocated_bytes
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
