use std::collections::hash_map::Entry;
use std::hash::{Hash, Hasher};

use super::super::normalizer::{normalize_label_key, normalize_label_value};
use super::super::{
    KeyValueRef, SeriesRef, U64HashMap, estimate_hashmap_table_bytes, estimate_vec_buffer_bytes,
};
use super::common::{LabelSetStore, LabelSetStoreError};

#[derive(Clone, Debug, Eq, PartialEq)]
struct OwnedKeyValue {
    key: String,
    value: String,
}

/// A deliberately naive layout that stores each labelset as its own `Vec<String>`.
///
/// This is used as a baseline to illustrate why a flat/arena-like layout
/// (`FlatInternedLabelSetStore`) is preferable for high-cardinality workloads:
/// millions of small allocations amplify allocator overhead and fragmentation,
/// and each series pays an extra `Vec` header (ptr/len/cap) plus per-string
/// heap allocations.
#[derive(Default)]
pub struct NaiveLabelSetStore {
    by_hash: U64HashMap<SeriesRef>,
    by_hash_collisions: U64HashMap<Vec<SeriesRef>>,
    series: Vec<Vec<OwnedKeyValue>>,
    estimated_collision_bytes: usize,
    series_vec_alloc_bytes: usize,
    series_vec_used_bytes: usize,
    series_string_alloc_bytes: usize,
    series_string_used_bytes: usize,
}

impl NaiveLabelSetStore {
    pub fn buffer_stats(&self) -> NaiveLabelSetStoreBufferStats {
        NaiveLabelSetStoreBufferStats {
            by_hash_len: self.by_hash.len(),
            by_hash_cap: self.by_hash.capacity(),
            by_hash_collisions_len: self.by_hash_collisions.len(),
            by_hash_collisions_cap: self.by_hash_collisions.capacity(),
            series_len: self.series.len(),
            series_cap: self.series.capacity(),
            series_vec_alloc_bytes: self.series_vec_alloc_bytes,
            series_vec_used_bytes: self.series_vec_used_bytes,
            series_string_alloc_bytes: self.series_string_alloc_bytes,
            series_string_used_bytes: self.series_string_used_bytes,
        }
    }

    fn series_slice(&self, series: SeriesRef) -> &[OwnedKeyValue] {
        &self.series[series.0 as usize]
    }

    fn labels_equal(stored: &[OwnedKeyValue], candidate: &[OwnedKeyValue]) -> bool {
        stored == candidate
    }

    fn encode(&self, labels: &[KeyValueRef<'_>]) -> (Vec<OwnedKeyValue>, u64) {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        let mut encoded: Vec<OwnedKeyValue> = Vec::with_capacity(labels.len());
        for label in labels {
            let key_norm = normalize_label_key(label.key);
            let value_norm = normalize_label_value(label.value);
            key_norm.as_ref().hash(&mut hasher);
            value_norm.as_ref().hash(&mut hasher);

            encoded.push(OwnedKeyValue {
                key: key_norm.into_owned(),
                value: value_norm.into_owned(),
            });
        }
        (encoded, hasher.finish())
    }
}

impl LabelSetStore for NaiveLabelSetStore {
    fn intern(&mut self, labels: &[KeyValueRef<'_>]) -> Result<SeriesRef, LabelSetStoreError> {
        debug_assert!(
            labels.windows(2).all(|pair| pair[0].key < pair[1].key),
            "LabelSet must be canonical (sorted by key, unique keys)"
        );

        let (encoded, labelset_hash) = self.encode(labels);

        if let Some(&candidate_series) = self.by_hash.get(&labelset_hash) {
            if Self::labels_equal(self.series_slice(candidate_series), &encoded) {
                return Ok(candidate_series);
            }

            if let Some(collisions) = self.by_hash_collisions.get(&labelset_hash) {
                for &candidate_series in collisions {
                    if Self::labels_equal(self.series_slice(candidate_series), &encoded) {
                        return Ok(candidate_series);
                    }
                }
            }
        }

        let series_ref = SeriesRef(self.series.len() as u32);

        self.series_vec_alloc_bytes = self.series_vec_alloc_bytes.saturating_add(
            encoded
                .capacity()
                .saturating_mul(std::mem::size_of::<OwnedKeyValue>()),
        );
        self.series_vec_used_bytes = self.series_vec_used_bytes.saturating_add(
            encoded
                .len()
                .saturating_mul(std::mem::size_of::<OwnedKeyValue>()),
        );
        for label in &encoded {
            self.series_string_alloc_bytes = self
                .series_string_alloc_bytes
                .saturating_add(label.key.capacity())
                .saturating_add(label.value.capacity());
            self.series_string_used_bytes = self
                .series_string_used_bytes
                .saturating_add(label.key.len())
                .saturating_add(label.value.len());
        }

        self.series.push(encoded);

        match self.by_hash.entry(labelset_hash) {
            Entry::Vacant(entry) => {
                entry.insert(series_ref);
            }
            Entry::Occupied(_) => {
                let collisions = self.by_hash_collisions.entry(labelset_hash).or_default();
                let before = collisions.capacity();
                collisions.push(series_ref);
                let after = collisions.capacity();
                if after > before {
                    self.estimated_collision_bytes = self.estimated_collision_bytes.saturating_add(
                        (after - before).saturating_mul(std::mem::size_of::<SeriesRef>()),
                    );
                }
            }
        }

        Ok(series_ref)
    }

    fn len(&self) -> usize {
        self.series.len()
    }

    fn visit_labelset(&self, series: SeriesRef, mut visitor: impl FnMut(&str, &str)) {
        let stored = self.series_slice(series);
        for label in stored.iter() {
            visitor(label.key.as_str(), label.value.as_str());
        }
    }

    fn estimate_size_bytes(&self) -> usize {
        let by_hash_bytes = estimate_hashmap_table_bytes(&self.by_hash)
            .saturating_add(estimate_hashmap_table_bytes(&self.by_hash_collisions));
        let by_hash_collision_heap_bytes = self.estimated_collision_bytes;
        let series_bytes = estimate_vec_buffer_bytes(&self.series);
        let series_values_bytes = self
            .series_vec_alloc_bytes
            .saturating_add(self.series_string_alloc_bytes);

        std::mem::size_of::<Self>()
            .saturating_add(by_hash_bytes)
            .saturating_add(by_hash_collision_heap_bytes)
            .saturating_add(series_bytes)
            .saturating_add(series_values_bytes)
    }

    fn estimate_used_bytes(&self) -> usize {
        let by_hash_bytes = self
            .by_hash
            .len()
            .saturating_mul(std::mem::size_of::<(u64, SeriesRef)>())
            .saturating_add(
                self.by_hash_collisions
                    .len()
                    .saturating_mul(std::mem::size_of::<(u64, Vec<SeriesRef>)>()),
            );

        let collision_bytes = self
            .by_hash_collisions
            .values()
            .map(|ids| ids.len().saturating_mul(std::mem::size_of::<SeriesRef>()))
            .fold(0usize, usize::saturating_add);

        let series_bytes = self
            .series
            .len()
            .saturating_mul(std::mem::size_of::<Vec<OwnedKeyValue>>());
        let series_values_bytes = self
            .series_vec_used_bytes
            .saturating_add(self.series_string_used_bytes);

        std::mem::size_of::<Self>()
            .saturating_add(by_hash_bytes)
            .saturating_add(collision_bytes)
            .saturating_add(series_bytes)
            .saturating_add(series_values_bytes)
    }
}

#[derive(Clone, Copy, Debug)]
pub struct NaiveLabelSetStoreBufferStats {
    pub by_hash_len: usize,
    pub by_hash_cap: usize,
    pub by_hash_collisions_len: usize,
    pub by_hash_collisions_cap: usize,
    pub series_len: usize,
    pub series_cap: usize,
    pub series_vec_alloc_bytes: usize,
    pub series_vec_used_bytes: usize,
    pub series_string_alloc_bytes: usize,
    pub series_string_used_bytes: usize,
}

impl std::fmt::Display for NaiveLabelSetStoreBufferStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "type={} by_hash_len={} by_hash_cap={} by_hash_collisions_len={} by_hash_collisions_cap={} series_len={} series_cap={} series_vec_alloc_bytes={} series_vec_used_bytes={} series_string_alloc_bytes={} series_string_used_bytes={}",
            super::NAIVE_BUFFER_STATS_TYPE_NAME,
            self.by_hash_len,
            self.by_hash_cap,
            self.by_hash_collisions_len,
            self.by_hash_collisions_cap,
            self.series_len,
            self.series_cap,
            self.series_vec_alloc_bytes,
            self.series_vec_used_bytes,
            self.series_string_alloc_bytes,
            self.series_string_used_bytes,
        )
    }
}
