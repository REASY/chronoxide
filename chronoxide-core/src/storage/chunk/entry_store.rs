use smallvec::SmallVec;

use super::types::ChunkIndexEntry;

pub(crate) trait SeriesChunkEntries:
    Default + AsRef<[ChunkIndexEntry]> + AsMut<[ChunkIndexEntry]>
{
    #[inline]
    fn as_slice(&self) -> &[ChunkIndexEntry] {
        self.as_ref()
    }

    #[inline]
    fn as_mut_slice(&mut self) -> &mut [ChunkIndexEntry] {
        self.as_mut()
    }

    fn push(&mut self, entry: ChunkIndexEntry);
}

impl SeriesChunkEntries for Vec<ChunkIndexEntry> {
    #[inline]
    fn push(&mut self, entry: ChunkIndexEntry) {
        Vec::push(self, entry);
    }
}

impl SeriesChunkEntries for SmallVec<[ChunkIndexEntry; 1]> {
    #[inline]
    fn push(&mut self, entry: ChunkIndexEntry) {
        SmallVec::push(self, entry);
    }
}

pub(crate) struct ChunkEntryStore<L: SeriesChunkEntries> {
    rows: Vec<L>,
}

#[cfg(test)]
pub(crate) type NestedVecChunkEntryStore = ChunkEntryStore<Vec<ChunkIndexEntry>>;
pub(crate) type InlineOneChunkEntryStore = ChunkEntryStore<SmallVec<[ChunkIndexEntry; 1]>>;

impl<L: SeriesChunkEntries> Default for ChunkEntryStore<L> {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

impl<L: SeriesChunkEntries> ChunkEntryStore<L> {
    #[inline]
    pub(crate) const fn new() -> Self {
        Self { rows: Vec::new() }
    }

    #[inline]
    #[cfg(test)]
    pub(crate) const fn from_rows(rows: Vec<L>) -> Self {
        Self { rows }
    }

    #[inline]
    pub(crate) fn into_rows(self) -> Vec<L> {
        self.rows
    }

    #[inline]
    pub(crate) fn len(&self) -> usize {
        self.rows.len()
    }

    #[inline]
    #[cfg(test)]
    pub(crate) fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    #[cfg(test)]
    #[inline]
    pub(crate) fn capacity(&self) -> usize {
        self.rows.capacity()
    }

    #[inline]
    pub(crate) fn reserve_series(&mut self, additional: usize) {
        self.rows.reserve(additional);
    }

    #[inline]
    pub(crate) fn push_empty_series(&mut self) {
        self.rows.push(L::default());
    }

    #[inline]
    #[track_caller]
    pub(crate) fn push_entry(&mut self, series_ref: usize, entry: ChunkIndexEntry) {
        self.rows
            .get_mut(series_ref)
            .expect("chunk entries length mismatch")
            .push(entry);
    }

    #[inline]
    #[track_caller]
    #[cfg(test)]
    pub(crate) fn series(&self, series_ref: usize) -> &[ChunkIndexEntry] {
        self.rows[series_ref].as_slice()
    }

    #[cfg(test)]
    #[inline]
    #[track_caller]
    pub(crate) fn series_mut(&mut self, series_ref: usize) -> &mut [ChunkIndexEntry] {
        self.rows[series_ref].as_mut_slice()
    }

    #[inline]
    pub(crate) fn rows(&self) -> &[L] {
        &self.rows
    }

    #[inline]
    #[cfg(test)]
    pub(crate) fn rows_mut(&mut self) -> &mut [L] {
        &mut self.rows
    }

    #[cfg(test)]
    #[inline]
    pub(crate) fn iter(
        &self,
    ) -> impl ExactSizeIterator<Item = &[ChunkIndexEntry]> + DoubleEndedIterator + '_ {
        self.rows.iter().map(SeriesChunkEntries::as_slice)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::chunk::ChunkKind;

    fn entry(offset: u64) -> ChunkIndexEntry {
        ChunkIndexEntry {
            file_id: 0,
            kind: ChunkKind::Float,
            flags: 0,
            min_time_ms: offset,
            max_time_ms: offset + 1,
            offset,
            length: 42,
            scalar_lane_offset: 0,
            scalar_lane_len: 0,
        }
    }

    fn assert_backend_conformance<L: SeriesChunkEntries>() {
        let mut store = ChunkEntryStore::<L>::new();
        assert!(store.is_empty());
        assert_eq!(store.len(), 0);

        store.reserve_series(3);
        assert!(store.capacity() >= 3);
        store.push_empty_series();
        store.push_empty_series();
        assert!(!store.is_empty());
        assert_eq!(store.len(), 2);
        assert!(store.series(0).is_empty());

        store.push_entry(0, entry(10));
        store.push_entry(0, entry(20));
        store.push_entry(1, entry(30));
        assert_eq!(
            store
                .iter()
                .map(|entries| entries.len())
                .collect::<Vec<_>>(),
            vec![2, 1]
        );
        assert_eq!(store.series(0)[1].offset, 20);
        assert_eq!(store.rows()[1].as_slice()[0].offset, 30);

        store.series_mut(0)[0].flags = 7;
        store.rows_mut()[1].as_mut_slice()[0].flags = 9;
        assert_eq!(store.series(0)[0].flags, 7);
        assert_eq!(store.series(1)[0].flags, 9);

        let rows = store.into_rows();
        let store = ChunkEntryStore::from_rows(rows);
        assert_eq!(store.len(), 2);
        assert_eq!(store.series(0).len(), 2);
        assert_eq!(store.series(1).len(), 1);
    }

    #[test]
    fn nested_vec_backend_conforms() {
        assert_backend_conformance::<Vec<ChunkIndexEntry>>();

        let _: NestedVecChunkEntryStore = ChunkEntryStore::new();
    }

    #[test]
    fn inline_one_backend_conforms_and_spills() {
        assert_backend_conformance::<SmallVec<[ChunkIndexEntry; 1]>>();

        let mut store = InlineOneChunkEntryStore::new();
        store.push_empty_series();
        store.push_entry(0, entry(10));
        assert!(!store.rows()[0].spilled());
        store.push_entry(0, entry(20));
        assert!(store.rows()[0].spilled());
        assert_eq!(
            store
                .series(0)
                .iter()
                .map(|entry| entry.offset)
                .collect::<Vec<_>>(),
            vec![10, 20]
        );
    }

    #[test]
    #[should_panic(expected = "chunk entries length mismatch")]
    fn push_entry_rejects_an_unregistered_series() {
        let mut store = InlineOneChunkEntryStore::new();

        store.push_entry(0, entry(10));
    }
}
