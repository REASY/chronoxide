use std::collections::{BTreeMap, btree_map};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use super::*;

#[test]
fn adaptive_table_matches_plain_hash_map_for_deterministic_trace() {
    for adaptive in [false, true] {
        let mut table = HeadSeriesTable::new(adaptive);
        let mut expected = BTreeMap::new();
        let mut state = 0x9e37_79b9_u32;

        for operation in 0..20_000_u32 {
            state = state.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
            let raw = match operation % 5 {
                0 => state & (PAGED_REF_LIMIT - 1),
                1 => (state % (PAGE_LEN as u32 * 3)) & !31,
                2 => (state % (PAGE_LEN as u32 * 3)) & !63,
                3 => PAGED_REF_LIMIT + (state % 16_384),
                _ => state,
            };
            let series = SeriesRef::new(raw);
            let value = operation as i64;

            match expected.entry(series) {
                btree_map::Entry::Occupied(_) => {
                    assert_eq!(table.insert_new(series, value), Err(value));
                }
                btree_map::Entry::Vacant(entry) => {
                    assert_eq!(table.insert_new(series, value), Ok(()));
                    entry.insert(value);
                }
            }

            if operation % 7 == 0 {
                let delta = i64::from(operation % 17);
                if let Some(value) = table.get_mut(series) {
                    *value += delta;
                    *expected.get_mut(&series).unwrap() += delta;
                }
            }
            assert_eq!(table.get(series), expected.get(&series));
            assert_eq!(table.len(), expected.len());
            assert_eq!(table.is_empty(), expected.is_empty());
            if operation % 509 == 0 {
                table.assert_stats_match_scan();
            }
        }

        table.assert_stats_match_scan();

        for value in table.values_mut() {
            *value = value.wrapping_add(11);
        }
        for value in expected.values_mut() {
            *value = value.wrapping_add(11);
        }

        let actual_keys: BTreeMap<_, _> = table
            .keys()
            .map(|series| (series, *table.get(series).unwrap()))
            .collect();
        assert_eq!(actual_keys, expected);

        let actual_iter: BTreeMap<_, _> = table
            .iter()
            .map(|(series, value)| (series, *value))
            .collect();
        assert_eq!(actual_iter, expected);

        let mut actual_values: Vec<_> = table.values().copied().collect();
        let mut expected_values: Vec<_> = expected.values().copied().collect();
        actual_values.sort_unstable();
        expected_values.sort_unstable();
        assert_eq!(actual_values, expected_values);

        let actual_consumed: BTreeMap<_, _> = table.into_entries().collect();
        assert_eq!(actual_consumed, expected);
    }
}

#[test]
fn page_promotes_exactly_at_128_entries() {
    let mut table = HeadSeriesTable::new(true);
    for raw in 0..(DIRECT_PAGE_THRESHOLD as u32 - 1) {
        table.insert_new(SeriesRef::new(raw), raw).unwrap();
    }
    assert_eq!(table.direct_page_count(), 0);
    assert_eq!(table.sparse_len(), DIRECT_PAGE_THRESHOLD - 1);

    let boundary = DIRECT_PAGE_THRESHOLD as u32 - 1;
    table
        .insert_new(SeriesRef::new(boundary), boundary)
        .unwrap();
    table.assert_stats_match_scan();
    assert_eq!(table.direct_page_count(), 1);
    assert_eq!(table.sparse_len(), 0);
    for raw in 0..DIRECT_PAGE_THRESHOLD as u32 {
        assert_eq!(table.get(SeriesRef::new(raw)), Some(&raw));
    }
}

#[test]
fn adjacent_refs_across_page_boundary_stay_distinct() {
    let mut table = HeadSeriesTable::new(true);
    let before = SeriesRef::new(PAGE_LEN as u32 - 1);
    let after = SeriesRef::new(PAGE_LEN as u32);
    table.insert_new(before, 11).unwrap();
    table.insert_new(after, 22).unwrap();

    assert_eq!(table.page_directory_len(), 2);
    assert_eq!(table.get(before), Some(&11));
    assert_eq!(table.get(after), Some(&22));
    assert_eq!(table.len(), 2);
}

#[test]
fn sparse_and_direct_pages_coexist_with_high_ref_fallback() {
    let mut table = HeadSeriesTable::new(true);
    let direct_base = PAGE_LEN as u32;
    for slot in 0..DIRECT_PAGE_THRESHOLD as u32 {
        table
            .insert_new(SeriesRef::new(direct_base + slot), slot)
            .unwrap();
    }
    let sparse = SeriesRef::new(PAGE_LEN as u32 * 3 + 17);
    let high = SeriesRef::new(PAGED_REF_LIMIT + 91);
    table.insert_new(sparse, 700).unwrap();
    table.insert_new(high, 800).unwrap();

    assert_eq!(table.direct_page_count(), 1);
    assert_eq!(table.sparse_len(), 2);
    assert_eq!(table.get(sparse), Some(&700));
    assert_eq!(table.get(high), Some(&800));
    assert_eq!(table.get(SeriesRef::new(direct_base + 73)), Some(&73));
}

#[test]
fn stride_32_promotes_while_stride_64_stays_sparse() {
    let mut stride_64 = HeadSeriesTable::new(true);
    for slot in (0..PAGE_LEN as u32).step_by(64) {
        stride_64.insert_new(SeriesRef::new(slot), slot).unwrap();
    }
    assert_eq!(stride_64.len(), 64);
    assert_eq!(stride_64.direct_page_count(), 0);
    assert_eq!(stride_64.sparse_len(), 64);

    let mut stride_32 = HeadSeriesTable::new(true);
    for slot in (0..PAGE_LEN as u32).step_by(32) {
        stride_32.insert_new(SeriesRef::new(slot), slot).unwrap();
    }
    assert_eq!(stride_32.len(), DIRECT_PAGE_THRESHOLD);
    assert_eq!(stride_32.direct_page_count(), 1);
    assert_eq!(stride_32.sparse_len(), 0);
}

#[test]
fn refs_at_or_above_limit_never_grow_page_directory() {
    let mut table = HeadSeriesTable::new(true);
    let refs = [
        SeriesRef::new(PAGED_REF_LIMIT),
        SeriesRef::new(PAGED_REF_LIMIT + 1),
        SeriesRef::new(u32::MAX),
    ];
    for (index, series) in refs.into_iter().enumerate() {
        table.insert_new(series, index).unwrap();
    }

    assert_eq!(table.page_directory_len(), 0);
    assert_eq!(table.direct_page_count(), 0);
    assert_eq!(table.sparse_len(), refs.len());
    for (index, series) in refs.into_iter().enumerate() {
        assert_eq!(table.get(series), Some(&index));
    }
    table.assert_stats_match_scan();
}

#[test]
fn maintained_stats_match_scan_across_removal_and_direct_growth() {
    for adaptive in [false, true] {
        let mut table = HeadSeriesTable::new(adaptive);
        let high = SeriesRef::new(PAGED_REF_LIMIT + 7);
        let sparse_page = SeriesRef::new(PAGE_LEN as u32 * 2 + 7);
        table.insert_new(high, 1).unwrap();
        table.insert_new(sparse_page, 2).unwrap();
        for raw in 0..(DIRECT_PAGE_THRESHOLD as u32 + 257) {
            table.insert_new(SeriesRef::new(raw), raw).unwrap();
        }
        table.assert_stats_match_scan();

        assert_eq!(table.remove(&high), Some(1));
        assert_eq!(table.remove(&sparse_page), Some(2));
        assert_eq!(table.remove(&SeriesRef::new(0)), Some(0));
        assert_eq!(table.remove(&SeriesRef::new(9_999_999)), None);
        table.assert_stats_match_scan();
    }
}

#[derive(Debug)]
struct DropProbe(Arc<AtomicUsize>);

impl Drop for DropProbe {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

#[test]
fn promotion_and_partial_consuming_iteration_drop_each_value_once() {
    let dropped = Arc::new(AtomicUsize::new(0));
    let mut table = HeadSeriesTable::new(true);
    let mut expected = 0;

    for raw in 0..DIRECT_PAGE_THRESHOLD as u32 {
        table
            .insert_new(SeriesRef::new(raw), DropProbe(Arc::clone(&dropped)))
            .unwrap();
        expected += 1;
    }
    for raw in [PAGE_LEN as u32 * 2 + 3, PAGED_REF_LIMIT + 9] {
        table
            .insert_new(SeriesRef::new(raw), DropProbe(Arc::clone(&dropped)))
            .unwrap();
        expected += 1;
    }
    assert_eq!(dropped.load(Ordering::Relaxed), 0);

    let mut entries = table.into_entries();
    let first = entries.next().unwrap();
    assert_eq!(dropped.load(Ordering::Relaxed), 0);
    drop(first);
    assert_eq!(dropped.load(Ordering::Relaxed), 1);
    drop(entries);
    assert_eq!(dropped.load(Ordering::Relaxed), expected);
}
