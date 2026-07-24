use super::*;

const WRITER_LABEL_PAGE_BYTES: usize = 64 * 1024;
const WRITER_LABELS_PER_PAGE: usize = WRITER_LABEL_PAGE_BYTES / std::mem::size_of::<(u32, u32)>();

#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct WriterSeriesRow {
    series_id: u64,
    label_location: u64,
    label_count: u32,
    kind_mask: u8,
    metadata_present: bool,
    reserved: [u8; 2],
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
struct WriterLabelArena {
    pages: Vec<Vec<(u32, u32)>>,
    len: usize,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(in crate::storage::segment) struct WriterSeriesEntryStore {
    rows: Vec<WriterSeriesRow>,
    labels: WriterLabelArena,
}

pub(super) struct WriterLabelAppender<'a> {
    labels: &'a mut Vec<(u32, u32)>,
}

impl WriterLabelAppender<'_> {
    pub(super) fn push(&mut self, label: (u32, u32)) {
        self.labels.push(label);
    }
}

impl WriterLabelArena {
    fn len(&self) -> usize {
        self.len
    }

    #[cfg(test)]
    fn capacity(&self) -> usize {
        self.pages
            .iter()
            .fold(0usize, |total, page| total.saturating_add(page.capacity()))
    }

    fn try_reserve_exact(&mut self, additional: usize) -> io::Result<()> {
        let remaining = self
            .pages
            .last()
            .map_or(0, |page| page.capacity().saturating_sub(page.len()));
        let new_pairs = additional.saturating_sub(remaining);
        let additional_pages = new_pairs.div_ceil(WRITER_LABELS_PER_PAGE);
        self.pages
            .try_reserve_exact(additional_pages)
            .map_err(|error| io::Error::new(io::ErrorKind::OutOfMemory, error))
    }

    fn write_row<T>(
        &mut self,
        expected_label_count: usize,
        write: impl FnOnce(&mut WriterLabelAppender<'_>) -> T,
    ) -> io::Result<(u64, u32, T)> {
        let label_count = u32::try_from(expected_label_count).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "series label count exceeds u32",
            )
        })?;
        let new_len = self.len.checked_add(expected_label_count).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "segment label-pair count exceeds usize",
            )
        })?;

        if expected_label_count == 0 {
            let mut empty = Vec::new();
            let value = write(&mut WriterLabelAppender { labels: &mut empty });
            if !empty.is_empty() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "encoded series label count does not match its reservation",
                ));
            }
            return Ok((0, label_count, value));
        }

        let use_last_page = self
            .pages
            .last()
            .is_some_and(|page| page.capacity().saturating_sub(page.len()) >= expected_label_count);
        let new_page = !use_last_page;
        if new_page {
            let page_capacity = WRITER_LABELS_PER_PAGE.max(expected_label_count);
            let mut page = Vec::new();
            page.try_reserve_exact(page_capacity)
                .map_err(|error| io::Error::new(io::ErrorKind::OutOfMemory, error))?;
            self.pages
                .try_reserve(1)
                .map_err(|error| io::Error::new(io::ErrorKind::OutOfMemory, error))?;
            self.pages.push(page);
        }

        let page_index = self.pages.len() - 1;
        let page_index_u32 = match u32::try_from(page_index) {
            Ok(page_index) => page_index,
            Err(_) => {
                if new_page {
                    self.pages.pop();
                }
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "writer label page index exceeds u32",
                ));
            }
        };
        let page = &mut self.pages[page_index];
        let start = page.len();
        let start_u32 = match u32::try_from(start) {
            Ok(start) => start,
            Err(_) => {
                if new_page {
                    self.pages.pop();
                }
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "writer label page offset exceeds u32",
                ));
            }
        };
        let value = write(&mut WriterLabelAppender { labels: page });
        let actual_label_count = page.len().saturating_sub(start);
        if actual_label_count != expected_label_count {
            page.truncate(start);
            if new_page {
                self.pages.pop();
            }
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "encoded series label count does not match its reservation",
            ));
        }

        self.len = new_len;
        Ok((
            pack_label_location(page_index_u32, start_u32),
            label_count,
            value,
        ))
    }

    fn get(&self, index: usize, row: WriterSeriesRow) -> io::Result<&[(u32, u32)]> {
        if row.label_count == 0 {
            return Ok(&[]);
        }
        let (page_index, start) = unpack_label_location(row.label_location);
        let count =
            usize::try_from(row.label_count).map_err(|_| invalid_series_label_range(index))?;
        let end = start
            .checked_add(count)
            .ok_or_else(|| invalid_series_label_range(index))?;
        self.pages
            .get(page_index)
            .and_then(|page| page.get(start..end))
            .ok_or_else(|| invalid_series_label_range(index))
    }

    fn get_mut(&mut self, index: usize, row: WriterSeriesRow) -> io::Result<&mut [(u32, u32)]> {
        if row.label_count == 0 {
            return Ok(&mut []);
        }
        let (page_index, start) = unpack_label_location(row.label_location);
        let count =
            usize::try_from(row.label_count).map_err(|_| invalid_series_label_range(index))?;
        let end = start
            .checked_add(count)
            .ok_or_else(|| invalid_series_label_range(index))?;
        self.pages
            .get_mut(page_index)
            .and_then(|page| page.get_mut(start..end))
            .ok_or_else(|| invalid_series_label_range(index))
    }

    fn try_append_to_row(
        &mut self,
        index: usize,
        row: WriterSeriesRow,
        label: (u32, u32),
    ) -> io::Result<bool> {
        if row.label_count == 0 {
            return Ok(false);
        }
        let (page_index, start) = unpack_label_location(row.label_location);
        let count =
            usize::try_from(row.label_count).map_err(|_| invalid_series_label_range(index))?;
        let end = start
            .checked_add(count)
            .ok_or_else(|| invalid_series_label_range(index))?;
        let page = self
            .pages
            .get_mut(page_index)
            .ok_or_else(|| invalid_series_label_range(index))?;
        if end > page.len() {
            return Err(invalid_series_label_range(index));
        }
        if end != page.len() || page.len() == page.capacity() {
            return Ok(false);
        }
        self.len = self.len.checked_add(1).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "segment label-pair count exceeds usize",
            )
        })?;
        page.push(label);
        Ok(true)
    }
}

fn pack_label_location(page_index: u32, offset: u32) -> u64 {
    (u64::from(page_index) << 32) | u64::from(offset)
}

fn unpack_label_location(location: u64) -> (usize, usize) {
    let page_index = (location >> 32) as u32;
    let offset = location as u32;
    (page_index as usize, offset as usize)
}

impl WriterSeriesEntryStore {
    pub(super) fn new() -> Self {
        Self::default()
    }

    #[cfg(test)]
    pub(in crate::storage::segment) fn from_owned(
        entries: Vec<WriterSeriesEntry>,
    ) -> io::Result<Self> {
        let mut store = Self::new();
        store.try_reserve_series(entries.len())?;
        let label_count = entries.iter().try_fold(0usize, |total, entry| {
            total.checked_add(entry.labels.len()).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidInput, "series label count overflow")
            })
        })?;
        store.try_reserve_label_page_directory(label_count)?;
        for entry in entries {
            let index = store.len();
            store.push_placeholder(entry.series_id, entry.kind_mask)?;
            store.set_metadata(index, entry.series_id, entry.kind_mask, &entry.labels)?;
        }
        Ok(store)
    }

    pub(super) fn len(&self) -> usize {
        self.rows.len()
    }

    pub(super) fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    pub(super) fn label_pair_count(&self) -> usize {
        self.labels.len()
    }

    #[cfg(test)]
    pub(in crate::storage::segment) fn rows_capacity(&self) -> usize {
        self.rows.capacity()
    }

    #[cfg(test)]
    pub(in crate::storage::segment) fn labels_len(&self) -> usize {
        self.labels.len()
    }

    #[cfg(test)]
    pub(in crate::storage::segment) fn labels_capacity(&self) -> usize {
        self.labels.capacity()
    }

    #[cfg(test)]
    pub(in crate::storage::segment) fn label_pairs(&self) -> Vec<(u32, u32)> {
        self.labels
            .pages
            .iter()
            .flat_map(|page| page.iter().copied())
            .collect()
    }

    pub(super) fn try_for_each_label_mut(
        &mut self,
        mut visit: impl FnMut(&mut (u32, u32)) -> io::Result<()>,
    ) -> io::Result<()> {
        for page in &mut self.labels.pages {
            for label in page {
                visit(label)?;
            }
        }
        Ok(())
    }

    pub(super) fn try_reserve_series(&mut self, additional: usize) -> io::Result<()> {
        self.rows
            .try_reserve(additional)
            .map_err(|error| io::Error::new(io::ErrorKind::OutOfMemory, error))
    }

    pub(super) fn try_reserve_series_exact(&mut self, additional: usize) -> io::Result<()> {
        self.rows
            .try_reserve_exact(additional)
            .map_err(|error| io::Error::new(io::ErrorKind::OutOfMemory, error))
    }

    pub(super) fn try_reserve_label_page_directory(&mut self, additional: usize) -> io::Result<()> {
        self.labels.try_reserve_exact(additional)
    }

    pub(super) fn push_placeholder(&mut self, series_id: u64, kind_mask: u8) -> io::Result<()> {
        self.try_reserve_series(1)?;
        self.rows.push(WriterSeriesRow {
            series_id,
            label_location: 0,
            label_count: 0,
            kind_mask,
            metadata_present: false,
            reserved: [0; 2],
        });
        Ok(())
    }

    pub(super) fn metadata_present(&self, index: usize) -> io::Result<bool> {
        self.rows
            .get(index)
            .map(|row| row.metadata_present)
            .ok_or_else(|| invalid_series_row(index))
    }

    pub(super) fn placeholder_source_ref(&self, index: usize) -> io::Result<u32> {
        let row = self
            .rows
            .get(index)
            .ok_or_else(|| invalid_series_row(index))?;
        if row.metadata_present {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("writer series row {index} no longer contains a placeholder source ref"),
            ));
        }
        u32::try_from(row.series_id).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("writer series row {index} placeholder source ref exceeds u32"),
            )
        })
    }

    pub(super) fn kind_mask(&self, index: usize) -> io::Result<u8> {
        self.rows
            .get(index)
            .map(|row| row.kind_mask)
            .ok_or_else(|| invalid_series_row(index))
    }

    pub(super) fn merge_kind(&mut self, index: usize, kind_mask: u8) -> io::Result<()> {
        let row = self
            .rows
            .get_mut(index)
            .ok_or_else(|| invalid_series_row(index))?;
        row.kind_mask |= kind_mask;
        Ok(())
    }

    #[cfg(test)]
    pub(super) fn set_metadata(
        &mut self,
        index: usize,
        series_id: u64,
        kind_mask: u8,
        labels: &[(u32, u32)],
    ) -> io::Result<()> {
        self.write_metadata(index, kind_mask, labels.len(), |appender| {
            for &label in labels {
                appender.push(label);
            }
            series_id
        })
    }

    pub(super) fn write_metadata<F>(
        &mut self,
        index: usize,
        kind_mask: u8,
        expected_label_count: usize,
        write: F,
    ) -> io::Result<()>
    where
        F: FnOnce(&mut WriterLabelAppender<'_>) -> u64,
    {
        self.write_metadata_inner(index, kind_mask, expected_label_count, write)
    }

    fn write_metadata_inner<F>(
        &mut self,
        index: usize,
        kind_mask: u8,
        expected_label_count: usize,
        write: F,
    ) -> io::Result<()>
    where
        F: FnOnce(&mut WriterLabelAppender<'_>) -> u64,
    {
        let row = self
            .rows
            .get(index)
            .copied()
            .ok_or_else(|| invalid_series_row(index))?;
        if row.metadata_present {
            return self.merge_kind(index, kind_mask);
        }

        let (label_location, label_count, series_id) =
            self.labels.write_row(expected_label_count, write)?;

        let row = self
            .rows
            .get_mut(index)
            .ok_or_else(|| invalid_series_row(index))?;
        row.series_id = series_id;
        row.label_location = label_location;
        row.label_count = label_count;
        row.kind_mask |= kind_mask;
        row.metadata_present = true;
        Ok(())
    }

    pub(super) fn labels_mut(&mut self, index: usize) -> io::Result<&mut [(u32, u32)]> {
        let row = self
            .rows
            .get(index)
            .copied()
            .ok_or_else(|| invalid_series_row(index))?;
        self.labels.get_mut(index, row)
    }

    pub(super) fn set_series_id(&mut self, index: usize, series_id: u64) -> io::Result<()> {
        let row = self
            .rows
            .get_mut(index)
            .ok_or_else(|| invalid_series_row(index))?;
        row.series_id = series_id;
        Ok(())
    }

    pub(super) fn append_label_to_row(
        &mut self,
        index: usize,
        label: (u32, u32),
    ) -> io::Result<()> {
        let row = self
            .rows
            .get(index)
            .copied()
            .ok_or_else(|| invalid_series_row(index))?;
        let new_count = row.label_count.checked_add(1).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "series label count exceeds u32",
            )
        })?;

        if self.labels.try_append_to_row(index, row, label)? {
            self.rows[index].label_count = new_count;
            return Ok(());
        }

        let mut labels = self.labels.get(index, row)?.to_vec();
        labels.push(label);
        let (label_location, label_count, ()) =
            self.labels.write_row(labels.len(), |appender| {
                for label in labels {
                    appender.push(label);
                }
            })?;
        let row = &mut self.rows[index];
        row.label_location = label_location;
        row.label_count = label_count;
        Ok(())
    }

    pub(super) fn reorder_rows_by_old_to_new_refs(
        mut self,
        old_to_new_refs: &[u32],
    ) -> io::Result<Self> {
        if self.rows.len() != old_to_new_refs.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "series entry count does not match the series-ref map",
            ));
        }

        let mut visited = vec![false; self.rows.len()];
        for &new_ref in old_to_new_refs {
            let new_index = usize::try_from(new_ref).map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidData, "series-ref map exceeds usize")
            })?;
            let Some(slot) = visited.get_mut(new_index) else {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "series-ref map contains an out-of-range ref",
                ));
            };
            if std::mem::replace(slot, true) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "series-ref map contains a duplicate ref",
                ));
            }
        }

        visited.fill(false);
        for start in 0..self.rows.len() {
            if visited[start] {
                continue;
            }
            let mut old_index = start;
            let mut carried = self.rows[old_index];
            loop {
                visited[old_index] = true;
                let new_index = old_to_new_refs[old_index] as usize;
                std::mem::swap(&mut carried, &mut self.rows[new_index]);
                old_index = new_index;
                if old_index == start {
                    break;
                }
            }
        }
        Ok(self)
    }
}

impl SeriesEntryStore for WriterSeriesEntryStore {
    fn len(&self) -> usize {
        self.rows.len()
    }

    fn get_entry(&self, index: usize) -> io::Result<SeriesEntryRef<'_>> {
        let row = self
            .rows
            .get(index)
            .copied()
            .ok_or_else(|| invalid_series_row(index))?;
        Ok(SeriesEntryRef::new(
            row.series_id,
            row.kind_mask,
            self.labels.get(index, row)?,
        ))
    }
}

fn invalid_series_row(index: usize) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        format!("writer series row {index} is missing"),
    )
}

fn invalid_series_label_range(index: usize) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        format!("writer series row {index} has an invalid label range"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(target_pointer_width = "64")]
    #[test]
    fn writer_series_row_stays_compact() {
        assert_eq!(std::mem::size_of::<WriterSeriesRow>(), 24);
        assert_eq!(std::mem::align_of::<WriterSeriesRow>(), 8);
    }

    #[test]
    fn row_reordering_keeps_flat_label_ranges_attached() {
        let mut entries = WriterSeriesEntryStore::new();
        entries.push_placeholder(10, SERIES_KIND_FLOAT).unwrap();
        entries.push_placeholder(20, SERIES_KIND_INT64).unwrap();
        entries
            .set_metadata(0, 100, SERIES_KIND_FLOAT, &[(1, 2), (3, 4)])
            .unwrap();
        entries
            .set_metadata(1, 200, SERIES_KIND_INT64, &[(5, 6)])
            .unwrap();
        let arena = entries.label_pairs();

        let entries = entries.reorder_rows_by_old_to_new_refs(&[1, 0]).unwrap();

        assert_eq!(entries.label_pairs(), arena);
        assert_eq!(entries.get_entry(0).unwrap().series_id(), 200);
        assert_eq!(entries.get_entry(0).unwrap().labels(), &[(5, 6)]);
        assert_eq!(entries.get_entry(1).unwrap().series_id(), 100);
        assert_eq!(entries.get_entry(1).unwrap().labels(), &[(1, 2), (3, 4)]);
    }

    #[test]
    fn row_reordering_handles_every_small_permutation() {
        fn visit_permutations(values: &mut [u32], index: usize, visit: &mut impl FnMut(&[u32])) {
            if index == values.len() {
                visit(values);
                return;
            }
            for swap_with in index..values.len() {
                values.swap(index, swap_with);
                visit_permutations(values, index + 1, visit);
                values.swap(index, swap_with);
            }
        }

        for len in 0..=6usize {
            let mut entries = WriterSeriesEntryStore::new();
            for old_index in 0..len {
                entries
                    .push_placeholder(100 + old_index as u64, SERIES_KIND_FLOAT)
                    .unwrap();
                entries
                    .set_metadata(
                        old_index,
                        1_000 + old_index as u64,
                        SERIES_KIND_FLOAT,
                        &[(old_index as u32, 10_000 + old_index as u32)],
                    )
                    .unwrap();
            }

            let mut old_to_new_refs = (0..len as u32).collect::<Vec<_>>();
            visit_permutations(&mut old_to_new_refs, 0, &mut |old_to_new_refs| {
                let reordered = entries
                    .clone()
                    .reorder_rows_by_old_to_new_refs(old_to_new_refs)
                    .unwrap();
                for (old_index, &new_ref) in old_to_new_refs.iter().enumerate() {
                    let entry = reordered.get_entry(new_ref as usize).unwrap();
                    assert_eq!(entry.series_id(), 1_000 + old_index as u64);
                    assert_eq!(
                        entry.labels(),
                        &[(old_index as u32, 10_000 + old_index as u32)]
                    );
                }
            });
        }
    }

    #[test]
    fn row_reordering_rejects_invalid_ref_maps() {
        let mut entries = WriterSeriesEntryStore::new();
        entries.push_placeholder(10, SERIES_KIND_FLOAT).unwrap();
        entries.push_placeholder(20, SERIES_KIND_FLOAT).unwrap();

        for invalid in [&[0][..], &[0, 0], &[0, 2]] {
            let error = entries
                .clone()
                .reorder_rows_by_old_to_new_refs(invalid)
                .unwrap_err();
            assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        }
    }

    #[test]
    fn repeated_metadata_only_merges_kind() {
        let mut entries = WriterSeriesEntryStore::new();
        entries.push_placeholder(10, SERIES_KIND_FLOAT).unwrap();
        entries
            .set_metadata(0, 100, SERIES_KIND_FLOAT, &[(1, 2)])
            .unwrap();
        let arena = entries.label_pairs();

        entries
            .set_metadata(0, 200, SERIES_KIND_INT64, &[(3, 4)])
            .unwrap();

        let entry = entries.get_entry(0).unwrap();
        assert_eq!(entry.series_id(), 100);
        assert_eq!(entry.kind_mask(), SERIES_KIND_FLOAT | SERIES_KIND_INT64);
        assert_eq!(entry.labels(), &[(1, 2)]);
        assert_eq!(entries.label_pairs(), arena);
    }

    #[test]
    fn appending_to_a_middle_row_repoints_only_that_row() {
        let mut entries = WriterSeriesEntryStore::new();
        entries.push_placeholder(10, SERIES_KIND_FLOAT).unwrap();
        entries.push_placeholder(20, SERIES_KIND_FLOAT).unwrap();
        entries
            .set_metadata(0, 100, SERIES_KIND_FLOAT, &[(1, 2)])
            .unwrap();
        entries
            .set_metadata(1, 200, SERIES_KIND_FLOAT, &[(3, 4)])
            .unwrap();

        entries.append_label_to_row(0, (5, 6)).unwrap();

        assert_eq!(entries.get_entry(0).unwrap().labels(), &[(1, 2), (5, 6)]);
        assert_eq!(entries.get_entry(1).unwrap().labels(), &[(3, 4)]);
    }

    #[test]
    fn invalid_middle_range_is_an_error_without_truncating_iteration() {
        let mut entries = WriterSeriesEntryStore::new();
        for series_id in 0..3 {
            entries
                .push_placeholder(series_id, SERIES_KIND_FLOAT)
                .unwrap();
            entries
                .set_metadata(
                    series_id as usize,
                    series_id,
                    SERIES_KIND_FLOAT,
                    &[(series_id as u32, 10)],
                )
                .unwrap();
        }
        entries.rows[1].label_location = u64::MAX;

        let mut iter = entries.entries();
        assert_eq!(iter.next().unwrap().unwrap().series_id(), 0);
        let error = iter.next().unwrap().unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert_eq!(iter.next().unwrap().unwrap().series_id(), 2);
        assert!(iter.next().is_none());
    }

    #[test]
    fn arena_page_growth_preserves_existing_row_ranges() {
        let mut entries = WriterSeriesEntryStore::new();
        entries.push_placeholder(1, SERIES_KIND_FLOAT).unwrap();
        entries
            .set_metadata(0, 1, SERIES_KIND_FLOAT, &[(1, 10), (2, 20)])
            .unwrap();
        let initial_capacity = entries.labels.capacity();

        for series_id in 2..700u64 {
            let index = entries.len();
            let labels = (0..16u32)
                .map(|label| (label + 10, label + series_id as u32))
                .collect::<Vec<_>>();
            entries
                .push_placeholder(series_id, SERIES_KIND_FLOAT)
                .unwrap();
            entries
                .set_metadata(index, series_id, SERIES_KIND_FLOAT, &labels)
                .unwrap();
        }

        assert!(entries.labels.capacity() > initial_capacity);
        assert_eq!(entries.get_entry(0).unwrap().labels(), &[(1, 10), (2, 20)]);
    }

    #[test]
    fn row_larger_than_the_default_page_remains_contiguous() {
        let mut entries = WriterSeriesEntryStore::new();
        entries.push_placeholder(1, SERIES_KIND_FLOAT).unwrap();
        entries.push_placeholder(2, SERIES_KIND_FLOAT).unwrap();
        let large_labels = (0..=WRITER_LABELS_PER_PAGE)
            .map(|label| (label as u32, (label + 1) as u32))
            .collect::<Vec<_>>();

        entries
            .set_metadata(0, 1, SERIES_KIND_FLOAT, &large_labels)
            .unwrap();
        entries
            .set_metadata(1, 2, SERIES_KIND_FLOAT, &[(7, 8)])
            .unwrap();

        assert_eq!(entries.labels.pages.len(), 2);
        assert_eq!(entries.get_entry(0).unwrap().labels(), large_labels);
        assert_eq!(entries.get_entry(1).unwrap().labels(), &[(7, 8)]);
    }

    #[test]
    fn row_boundary_spill_preserves_both_page_ranges() {
        let mut entries = WriterSeriesEntryStore::new();
        entries.push_placeholder(1, SERIES_KIND_FLOAT).unwrap();
        entries.push_placeholder(2, SERIES_KIND_FLOAT).unwrap();
        let first_labels = (0..WRITER_LABELS_PER_PAGE - 1)
            .map(|label| (label as u32, (label + 1) as u32))
            .collect::<Vec<_>>();

        entries
            .set_metadata(0, 1, SERIES_KIND_FLOAT, &first_labels)
            .unwrap();
        entries
            .set_metadata(1, 2, SERIES_KIND_FLOAT, &[(7, 8), (9, 10)])
            .unwrap();

        assert_eq!(entries.labels.pages.len(), 2);
        assert_eq!(entries.labels.pages[0].len(), WRITER_LABELS_PER_PAGE - 1);
        assert_eq!(entries.labels.pages[1].len(), 2);
        assert_eq!(entries.get_entry(0).unwrap().labels(), first_labels);
        assert_eq!(entries.get_entry(1).unwrap().labels(), &[(7, 8), (9, 10)]);
    }

    #[test]
    fn corrupt_page_index_and_offset_are_errors() {
        let mut entries = WriterSeriesEntryStore::new();
        entries.push_placeholder(1, SERIES_KIND_FLOAT).unwrap();
        entries
            .set_metadata(0, 1, SERIES_KIND_FLOAT, &[(1, 10)])
            .unwrap();

        entries.rows[0].label_location = pack_label_location(u32::MAX, 0);
        assert_eq!(
            entries.get_entry(0).unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );

        entries.rows[0].label_location = pack_label_location(0, u32::MAX);
        assert_eq!(
            entries.get_entry(0).unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );
    }

    #[cfg(target_pointer_width = "64")]
    #[test]
    fn impossible_page_directory_reservation_does_not_change_rows_or_labels() {
        let mut entries = WriterSeriesEntryStore::new();
        entries.push_placeholder(1, SERIES_KIND_FLOAT).unwrap();
        entries
            .set_metadata(0, 1, SERIES_KIND_FLOAT, &[(1, 10)])
            .unwrap();
        let expected = entries.clone();

        let error = entries
            .try_reserve_label_page_directory(usize::MAX)
            .unwrap_err();

        assert_eq!(error.kind(), io::ErrorKind::OutOfMemory);
        assert_eq!(entries, expected);
    }

    #[test]
    fn direct_metadata_count_mismatch_rolls_back_the_arena_and_row() {
        let mut entries = WriterSeriesEntryStore::new();
        entries.push_placeholder(1, SERIES_KIND_FLOAT).unwrap();

        let error = entries
            .write_metadata(0, SERIES_KIND_FLOAT, 2, |appender| {
                appender.push((1, 10));
                99
            })
            .unwrap_err();

        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert_eq!(entries.labels_len(), 0);
        assert!(!entries.metadata_present(0).unwrap());
        assert_eq!(entries.get_entry(0).unwrap().series_id(), 1);
    }
}
