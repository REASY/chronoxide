//! Persistent ID-based catalog for immutable live-head query views.
//!
//! Label and symbol bytes remain owned by
//! [`VersionedFlatInternedLabelSetSnapshot`].  This module path-copies only
//! active-series metadata and inverted-index memberships, so publishing a new
//! view neither clones the label corpus nor mutates an older pinned revision.

use std::cmp::Ordering;
use std::fmt;
use std::io;
use std::sync::Arc;

use crate::hash::xxhash64;
use crate::labels::{
    LabelSetStore, METRIC_NAME_LABEL, SeriesRef, SymbolId, VersionedFlatInternedLabelSetSnapshot,
    VersionedFlatLabelStoreError,
};
use crate::promql::{normalize_label_name, normalize_metric_name};

use super::{
    CompiledLabelMatcher, LiveSampleStore, NormalizedMatcher, QueryBudget, compile_label_matchers,
    compile_promql_regex, intersect_sorted, promql_projection_metric_name_matches, subtract_sorted,
    union_sorted,
};

type MapLink<K, V> = Option<Arc<MapNode<K, V>>>;

#[derive(Clone)]
struct PersistentMap<K, V> {
    root: MapLink<K, V>,
}

impl<K, V> Default for PersistentMap<K, V> {
    fn default() -> Self {
        Self { root: None }
    }
}

struct MapNode<K, V> {
    key: K,
    value: V,
    left: MapLink<K, V>,
    right: MapLink<K, V>,
    height: u16,
    entries: u64,
}

impl<K, V> fmt::Debug for PersistentMap<K, V>
where
    K: fmt::Debug,
    V: fmt::Debug,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PersistentMap")
            .field("entries", &map_entries(&self.root))
            .field("height", &map_height(&self.root))
            .finish()
    }
}

impl<K, V> MapNode<K, V> {
    fn make(key: K, value: V, left: MapLink<K, V>, right: MapLink<K, V>) -> io::Result<Arc<Self>> {
        let height = map_height(&left)
            .max(map_height(&right))
            .checked_add(1)
            .ok_or_else(|| invalid_data("live catalog map height overflows u16"))?;
        let entries = map_entries(&left)
            .checked_add(map_entries(&right))
            .and_then(|entries| entries.checked_add(1))
            .ok_or_else(|| invalid_data("live catalog map entry count overflows u64"))?;
        Ok(Arc::new(Self {
            key,
            value,
            left,
            right,
            height,
            entries,
        }))
    }
}

fn map_height<K, V>(link: &MapLink<K, V>) -> u16 {
    link.as_ref().map_or(0, |node| node.height)
}

fn map_entries<K, V>(link: &MapLink<K, V>) -> u64 {
    link.as_ref().map_or(0, |node| node.entries)
}

impl<K, V> PersistentMap<K, V>
where
    K: Ord + Clone,
    V: Clone,
{
    fn get(&self, key: &K) -> Option<&V> {
        let mut current = self.root.as_deref();
        while let Some(node) = current {
            match key.cmp(&node.key) {
                Ordering::Less => current = node.left.as_deref(),
                Ordering::Greater => current = node.right.as_deref(),
                Ordering::Equal => return Some(&node.value),
            }
        }
        None
    }

    fn insert(&mut self, key: K, value: V) -> io::Result<()> {
        self.root = Some(map_insert(&self.root, key, value)?);
        Ok(())
    }

    fn remove(&mut self, key: &K) -> io::Result<bool> {
        let (root, removed) = map_remove(&self.root, key)?;
        self.root = root;
        Ok(removed)
    }

    fn entries(&self) -> io::Result<Vec<(&K, &V)>> {
        map_in_order(&self.root)
    }

    fn range_entries(&self, lower: &K, upper: &K) -> io::Result<Vec<(&K, &V)>> {
        if lower > upper {
            return Err(invalid_data("live catalog map range is reversed"));
        }
        let mut count = 0usize;
        map_visit_range(&self.root, lower, upper, |_, _| {
            count = count
                .checked_add(1)
                .ok_or_else(|| invalid_data("live catalog range count exceeds usize"))?;
            Ok(())
        })?;
        let mut entries = Vec::new();
        entries
            .try_reserve_exact(count)
            .map_err(|error| io::Error::new(io::ErrorKind::OutOfMemory, error))?;
        map_visit_range(&self.root, lower, upper, |key, value| {
            entries.push((key, value));
            Ok(())
        })?;
        Ok(entries)
    }
}

fn map_insert<K, V>(root: &MapLink<K, V>, key: K, value: V) -> io::Result<Arc<MapNode<K, V>>>
where
    K: Ord + Clone,
    V: Clone,
{
    let Some(node) = root else {
        return MapNode::make(key, value, None, None);
    };
    let rebuilt = match key.cmp(&node.key) {
        Ordering::Less => MapNode::make(
            node.key.clone(),
            node.value.clone(),
            Some(map_insert(&node.left, key, value)?),
            node.right.clone(),
        )?,
        Ordering::Greater => MapNode::make(
            node.key.clone(),
            node.value.clone(),
            node.left.clone(),
            Some(map_insert(&node.right, key, value)?),
        )?,
        Ordering::Equal => {
            return MapNode::make(key, value, node.left.clone(), node.right.clone());
        }
    };
    map_balance(rebuilt)
}

fn map_remove<K, V>(root: &MapLink<K, V>, key: &K) -> io::Result<(MapLink<K, V>, bool)>
where
    K: Ord + Clone,
    V: Clone,
{
    let Some(node) = root else {
        return Ok((None, false));
    };
    let (rebuilt, removed) = match key.cmp(&node.key) {
        Ordering::Less => {
            let (left, removed) = map_remove(&node.left, key)?;
            if !removed {
                return Ok((root.clone(), false));
            }
            (
                Some(MapNode::make(
                    node.key.clone(),
                    node.value.clone(),
                    left,
                    node.right.clone(),
                )?),
                true,
            )
        }
        Ordering::Greater => {
            let (right, removed) = map_remove(&node.right, key)?;
            if !removed {
                return Ok((root.clone(), false));
            }
            (
                Some(MapNode::make(
                    node.key.clone(),
                    node.value.clone(),
                    node.left.clone(),
                    right,
                )?),
                true,
            )
        }
        Ordering::Equal => match (&node.left, &node.right) {
            (None, None) => (None, true),
            (Some(_), None) => (node.left.clone(), true),
            (None, Some(_)) => (node.right.clone(), true),
            (Some(_), Some(right)) => {
                let successor = map_min(right);
                let (right, removed) = map_remove(&node.right, &successor.key)?;
                if !removed {
                    return Err(invalid_data(
                        "live catalog map successor disappeared during removal",
                    ));
                }
                (
                    Some(MapNode::make(
                        successor.key.clone(),
                        successor.value.clone(),
                        node.left.clone(),
                        right,
                    )?),
                    true,
                )
            }
        },
    };
    match rebuilt {
        Some(node) => Ok((Some(map_balance(node)?), removed)),
        None => Ok((None, removed)),
    }
}

fn map_min<K, V>(mut node: &Arc<MapNode<K, V>>) -> &Arc<MapNode<K, V>> {
    while let Some(left) = &node.left {
        node = left;
    }
    node
}

fn map_balance<K, V>(node: Arc<MapNode<K, V>>) -> io::Result<Arc<MapNode<K, V>>>
where
    K: Ord + Clone,
    V: Clone,
{
    let balance = i32::from(map_height(&node.left)) - i32::from(map_height(&node.right));
    if balance > 1 {
        let left = node
            .left
            .as_ref()
            .ok_or_else(|| invalid_data("left-heavy live catalog map has no left child"))?;
        if map_height(&left.left) < map_height(&left.right) {
            let left = map_rotate_left(Arc::clone(left))?;
            return map_rotate_right(MapNode::make(
                node.key.clone(),
                node.value.clone(),
                Some(left),
                node.right.clone(),
            )?);
        }
        return map_rotate_right(node);
    }
    if balance < -1 {
        let right = node
            .right
            .as_ref()
            .ok_or_else(|| invalid_data("right-heavy live catalog map has no right child"))?;
        if map_height(&right.right) < map_height(&right.left) {
            let right = map_rotate_right(Arc::clone(right))?;
            return map_rotate_left(MapNode::make(
                node.key.clone(),
                node.value.clone(),
                node.left.clone(),
                Some(right),
            )?);
        }
        return map_rotate_left(node);
    }
    Ok(node)
}

fn map_rotate_left<K, V>(root: Arc<MapNode<K, V>>) -> io::Result<Arc<MapNode<K, V>>>
where
    K: Ord + Clone,
    V: Clone,
{
    let pivot = root
        .right
        .as_ref()
        .ok_or_else(|| invalid_data("cannot rotate live catalog map left without right child"))?;
    let left = MapNode::make(
        root.key.clone(),
        root.value.clone(),
        root.left.clone(),
        pivot.left.clone(),
    )?;
    MapNode::make(
        pivot.key.clone(),
        pivot.value.clone(),
        Some(left),
        pivot.right.clone(),
    )
}

fn map_rotate_right<K, V>(root: Arc<MapNode<K, V>>) -> io::Result<Arc<MapNode<K, V>>>
where
    K: Ord + Clone,
    V: Clone,
{
    let pivot = root
        .left
        .as_ref()
        .ok_or_else(|| invalid_data("cannot rotate live catalog map right without left child"))?;
    let right = MapNode::make(
        root.key.clone(),
        root.value.clone(),
        pivot.right.clone(),
        root.right.clone(),
    )?;
    MapNode::make(
        pivot.key.clone(),
        pivot.value.clone(),
        pivot.left.clone(),
        Some(right),
    )
}

fn map_in_order<K: Ord, V>(root: &MapLink<K, V>) -> io::Result<Vec<(&K, &V)>> {
    let capacity = usize::from(map_height(root));
    let expected = usize::try_from(map_entries(root))
        .map_err(|_| invalid_data("live catalog map entry count exceeds usize"))?;
    let mut stack = Vec::new();
    stack
        .try_reserve_exact(capacity)
        .map_err(|error| io::Error::new(io::ErrorKind::OutOfMemory, error))?;
    let mut entries = Vec::new();
    entries
        .try_reserve_exact(expected)
        .map_err(|error| io::Error::new(io::ErrorKind::OutOfMemory, error))?;

    let mut current = root.as_deref();
    let mut previous = None;
    while current.is_some() || !stack.is_empty() {
        while let Some(node) = current {
            validate_map_node(node)?;
            if stack.len() >= capacity {
                return Err(invalid_data(
                    "live catalog traversal exceeds validated map height",
                ));
            }
            stack.push(node);
            current = node.left.as_deref();
        }
        let node = stack
            .pop()
            .ok_or_else(|| invalid_data("live catalog map traversal stack underflow"))?;
        if previous.is_some_and(|key| key >= &node.key) {
            return Err(invalid_data(
                "live catalog map keys are not strictly ordered",
            ));
        }
        previous = Some(&node.key);
        entries.push((&node.key, &node.value));
        current = node.right.as_deref();
    }
    if entries.len() != expected {
        return Err(invalid_data(
            "live catalog map traversal disagrees with root count",
        ));
    }
    Ok(entries)
}

fn map_visit_range<'a, K, V>(
    root: &'a MapLink<K, V>,
    lower: &K,
    upper: &K,
    mut visitor: impl FnMut(&'a K, &'a V) -> io::Result<()>,
) -> io::Result<()>
where
    K: Ord,
{
    let capacity = usize::from(map_height(root));
    let mut stack = Vec::new();
    stack
        .try_reserve_exact(capacity)
        .map_err(|error| io::Error::new(io::ErrorKind::OutOfMemory, error))?;
    let mut current = root.as_deref();
    while current.is_some() || !stack.is_empty() {
        while let Some(node) = current {
            validate_map_node(node)?;
            if node.key < *lower {
                current = node.right.as_deref();
            } else {
                if stack.len() >= capacity {
                    return Err(invalid_data(
                        "live catalog range traversal exceeds validated map height",
                    ));
                }
                stack.push(node);
                current = node.left.as_deref();
            }
        }
        let node = stack
            .pop()
            .ok_or_else(|| invalid_data("live catalog range traversal stack underflow"))?;
        if node.key > *upper {
            break;
        }
        visitor(&node.key, &node.value)?;
        current = node.right.as_deref();
    }
    Ok(())
}

fn validate_map_node<K, V>(node: &MapNode<K, V>) -> io::Result<()> {
    let height = map_height(&node.left)
        .max(map_height(&node.right))
        .checked_add(1)
        .ok_or_else(|| invalid_data("live catalog map height overflows u16"))?;
    let entries = map_entries(&node.left)
        .checked_add(map_entries(&node.right))
        .and_then(|entries| entries.checked_add(1))
        .ok_or_else(|| invalid_data("live catalog map entry count overflows u64"))?;
    let balance = i32::from(map_height(&node.left)) - i32::from(map_height(&node.right));
    if height != node.height || entries != node.entries || balance.abs() > 1 {
        return Err(invalid_data(
            "live catalog map metadata or AVL balance is invalid",
        ));
    }
    Ok(())
}

#[derive(Clone, Debug)]
struct ActiveSeries {
    series_id: u64,
    row_revision: u64,
    born_generation: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct PostingKey {
    name: SymbolId,
    value: SymbolId,
    series: SeriesRef,
}

#[derive(Clone, Debug)]
struct PostingMembership {
    row_revision: u64,
    born_generation: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct LabelValueKey {
    name: SymbolId,
    value: SymbolId,
}

#[derive(Clone, Debug)]
struct LabelValueMembership {
    active_series: u32,
    row_revision: u64,
    born_generation: u64,
}

#[derive(Clone, Debug)]
struct LabelNameMembership {
    active_series: u32,
    row_revision: u64,
    born_generation: u64,
}

/// Immutable, revision-filtered live-series identity and inverted-index root.
///
/// The root owns no label strings. Every query-visible string is resolved
/// from `labels` only after exact time presence and matcher selection.
#[derive(Clone)]
pub struct LiveSeriesCatalog {
    labels: Arc<VersionedFlatInternedLabelSetSnapshot>,
    generation: u64,
    active: PersistentMap<SeriesRef, ActiveSeries>,
    postings: PersistentMap<PostingKey, PostingMembership>,
    names: PersistentMap<SymbolId, LabelNameMembership>,
    values: PersistentMap<LabelValueKey, LabelValueMembership>,
}

/// Conservative per-root bytes if none of this catalog's allocations were
/// shared with another generation.
///
/// These values are deliberately not an exclusive-retained-byte lease:
/// immutable label pages and unchanged persistent map nodes can be owned by
/// several pinned generations. A governor must deduplicate allocation
/// identities or account only candidate deltas before using this estimate.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct LiveSeriesCatalogMemoryEstimate {
    pub shared_label_snapshot_bytes: u64,
    pub catalog_index_bytes_if_unshared: u64,
    pub total_bytes_if_unshared: u64,
}

impl fmt::Debug for LiveSeriesCatalog {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LiveSeriesCatalog")
            .field("revision", &self.revision())
            .field("generation", &self.generation)
            .field("active_series", &map_entries(&self.active.root))
            .field("posting_memberships", &map_entries(&self.postings.root))
            .field("active_label_names", &map_entries(&self.names.root))
            .field("active_label_values", &map_entries(&self.values.root))
            .finish()
    }
}

impl LiveSeriesCatalog {
    pub fn revision(&self) -> u64 {
        self.labels.revision()
    }

    pub fn generation(&self) -> u64 {
        self.generation
    }

    pub fn labels(&self) -> &Arc<VersionedFlatInternedLabelSetSnapshot> {
        &self.labels
    }

    pub fn active_series_len(&self) -> u64 {
        map_entries(&self.active.root)
    }

    pub fn memory_estimate(&self) -> LiveSeriesCatalogMemoryEstimate {
        let shared_label_snapshot_bytes =
            u64::try_from(LabelSetStore::estimate_size_bytes(self.labels.as_ref()))
                .unwrap_or(u64::MAX);
        let catalog_index_bytes_if_unshared =
            map_allocated_bytes::<SeriesRef, ActiveSeries>(map_entries(&self.active.root))
                .saturating_add(map_allocated_bytes::<PostingKey, PostingMembership>(
                    map_entries(&self.postings.root),
                ))
                .saturating_add(map_allocated_bytes::<SymbolId, LabelNameMembership>(
                    map_entries(&self.names.root),
                ))
                .saturating_add(map_allocated_bytes::<LabelValueKey, LabelValueMembership>(
                    map_entries(&self.values.root),
                ))
                .saturating_add(u64::try_from(std::mem::size_of::<Self>()).unwrap_or(u64::MAX));
        LiveSeriesCatalogMemoryEstimate {
            shared_label_snapshot_bytes,
            catalog_index_bytes_if_unshared,
            total_bytes_if_unshared: shared_label_snapshot_bytes
                .saturating_add(catalog_index_bytes_if_unshared),
        }
    }

    pub fn estimated_bytes(&self) -> u64 {
        self.memory_estimate().total_bytes_if_unshared
    }

    pub fn active_series_refs(&self) -> io::Result<Vec<SeriesRef>> {
        let entries = self.active.entries()?;
        let mut series = Vec::new();
        series
            .try_reserve_exact(entries.len())
            .map_err(|error| io::Error::new(io::ErrorKind::OutOfMemory, error))?;
        series.extend(entries.into_iter().map(|(series, _)| *series));
        Ok(series)
    }

    pub fn validate_sample_store(&self, samples: &LiveSampleStore) -> io::Result<()> {
        if samples.required_catalog_revision() > self.revision() {
            return Err(invalid_data(format!(
                "live samples require catalog revision {}, but catalog revision is {}",
                samples.required_catalog_revision(),
                self.revision()
            )));
        }
        let sample_series = samples.active_series_refs()?;
        let catalog_series = self.active_series_refs()?;
        if sample_series != catalog_series {
            return Err(invalid_data(
                "live series catalog active set does not exactly match the sample root",
            ));
        }
        Ok(())
    }

    /// Returns the stable ID of one active canonical PromQL label identity.
    ///
    /// The ID is an accelerator, not an equality proof. Call
    /// [`canonical_series_identity_eq`](Self::canonical_series_identity_eq)
    /// before treating two matching IDs as the same identity.
    pub fn series_id(&self, series: SeriesRef) -> io::Result<Option<u64>> {
        let Some(active) = self.active.get(&series) else {
            return Ok(None);
        };
        if active.row_revision > self.revision() || active.born_generation > self.generation {
            return Err(invalid_data(
                "live series entry is newer than its pinned catalog cut",
            ));
        }
        Ok(Some(active.series_id))
    }

    /// Verifies complete canonical-label equality for two active series.
    pub fn canonical_series_identity_eq(
        &self,
        left: SeriesRef,
        right: SeriesRef,
    ) -> io::Result<bool> {
        if self.series_id(left)?.is_none() || self.series_id(right)?.is_none() {
            return Err(invalid_data(
                "cannot compare canonical identity for an inactive live series",
            ));
        }
        let left = self
            .labels
            .try_canonical_labelset_symbol_ids(left)
            .map_err(label_error)?;
        let right = self
            .labels
            .try_canonical_labelset_symbol_ids(right)
            .map_err(label_error)?;
        Ok(left.len() == right.len() && left.iter().eq(right.iter()))
    }

    pub(crate) fn materialize_labels(
        &self,
        series: SeriesRef,
    ) -> io::Result<Vec<(String, String)>> {
        if self.series_id(series)?.is_none() {
            return Err(invalid_data(
                "cannot materialize labels for an inactive live series",
            ));
        }
        let row = self
            .labels
            .try_canonical_labelset_symbol_ids(series)
            .map_err(label_error)?;
        let mut labels = Vec::new();
        labels
            .try_reserve_exact(row.len())
            .map_err(|error| io::Error::new(io::ErrorKind::OutOfMemory, error))?;
        for (name, value) in row.iter() {
            let name = self
                .labels
                .symbols()
                .try_resolve(name)
                .map_err(label_error)?;
            let value = self
                .labels
                .symbols()
                .try_resolve(value)
                .map_err(label_error)?;
            labels.push((
                try_clone_string(name, "live result label name")?,
                try_clone_string(value, "live result label value")?,
            ));
        }
        Ok(labels)
    }

    pub(crate) fn matching_series(
        &self,
        present: &[SeriesRef],
        matchers: &[NormalizedMatcher],
        budget: &mut QueryBudget,
        match_promql_projection_names: bool,
    ) -> io::Result<Vec<SeriesRef>> {
        validate_sorted_presence(present)?;
        for series in present {
            if self.series_id(*series)?.is_none() {
                return Err(invalid_data(
                    "time-presence set contains a series absent from the live catalog",
                ));
            }
        }

        let compiled_matchers = compile_label_matchers(matchers)?;
        let mut candidates: Option<Vec<SeriesRef>> = None;
        for (matcher, compiled) in matchers.iter().zip(&compiled_matchers) {
            if compiled.requires_missing_label_scan() {
                if let NormalizedMatcher::Regex { name, .. }
                | NormalizedMatcher::NotRegex { name, .. } = matcher
                {
                    self.charge_present_regex_values(name, present, budget)?;
                }
                continue;
            }
            let positive = match matcher {
                NormalizedMatcher::Eq { name, value } => {
                    Some(self.exact_postings(name, value, present)?)
                }
                NormalizedMatcher::Regex { name, pattern } => Some(self.regex_postings(
                    name,
                    pattern,
                    present,
                    budget,
                    match_promql_projection_names && name == METRIC_NAME_LABEL,
                )?),
                NormalizedMatcher::NotEq { .. } | NormalizedMatcher::NotRegex { .. } => None,
            };
            if let Some(positive) = positive {
                if positive.is_empty() {
                    return Ok(Vec::new());
                }
                candidates = Some(match candidates {
                    Some(existing) => intersect_sorted(&existing, &positive),
                    None => positive,
                });
            }
        }

        let mut candidates = match candidates {
            Some(candidates) => candidates,
            None => {
                let mut candidates = Vec::new();
                candidates
                    .try_reserve_exact(present.len())
                    .map_err(|error| io::Error::new(io::ErrorKind::OutOfMemory, error))?;
                candidates.extend_from_slice(present);
                candidates
            }
        };
        for (matcher, compiled) in matchers.iter().zip(&compiled_matchers) {
            if compiled.requires_missing_label_scan() {
                continue;
            }
            match matcher {
                NormalizedMatcher::NotEq { name, value } => {
                    let posting = self.exact_postings(name, value, present)?;
                    candidates = subtract_sorted(&candidates, &posting);
                }
                NormalizedMatcher::NotRegex { name, pattern } => {
                    let posting = self.regex_postings(name, pattern, present, budget, false)?;
                    candidates = subtract_sorted(&candidates, &posting);
                }
                NormalizedMatcher::Eq { .. } | NormalizedMatcher::Regex { .. } => {}
            }
        }

        if compiled_matchers
            .iter()
            .any(CompiledLabelMatcher::requires_missing_label_scan)
        {
            let mut filtered = Vec::new();
            filtered
                .try_reserve_exact(candidates.len())
                .map_err(|error| io::Error::new(io::ErrorKind::OutOfMemory, error))?;
            for series in candidates {
                let mut matches = true;
                for matcher in compiled_matchers
                    .iter()
                    .filter(|matcher| matcher.requires_missing_label_scan())
                {
                    let value = self.label_value(series, matcher.name())?.unwrap_or("");
                    if !matcher.matches_value(value, match_promql_projection_names) {
                        matches = false;
                        break;
                    }
                }
                if matches {
                    filtered.push(series);
                }
            }
            candidates = filtered;
        }
        Ok(candidates)
    }

    fn exact_postings(
        &self,
        name: &str,
        value: &str,
        present: &[SeriesRef],
    ) -> io::Result<Vec<SeriesRef>> {
        let Some((name_id, value_id)) = self.resolve_active_pair(name, value)? else {
            return Ok(Vec::new());
        };
        self.postings_for_ids(name_id, value_id, present)
    }

    fn regex_postings(
        &self,
        name: &str,
        pattern: &str,
        present: &[SeriesRef],
        budget: &mut QueryBudget,
        match_promql_projection_names: bool,
    ) -> io::Result<Vec<SeriesRef>> {
        let regex = compile_promql_regex(pattern)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;
        let Some(name_id) = self.resolve_active_name(name)? else {
            return Ok(Vec::new());
        };
        let mut out = Vec::new();
        for (value_id, value, posting) in self.present_values(name_id, present)? {
            let _ = value_id;
            budget.observe_regex_value()?;
            let matches = if match_promql_projection_names {
                promql_projection_metric_name_matches(value, &regex)
            } else {
                regex.is_match(value)
            };
            if matches {
                out = union_sorted(&out, &posting);
            }
        }
        Ok(out)
    }

    fn charge_present_regex_values(
        &self,
        name: &str,
        present: &[SeriesRef],
        budget: &mut QueryBudget,
    ) -> io::Result<()> {
        let Some(name_id) = self.resolve_active_name(name)? else {
            return Ok(());
        };
        for _ in self.present_values(name_id, present)? {
            budget.observe_regex_value()?;
        }
        Ok(())
    }

    fn resolve_active_name(&self, name: &str) -> io::Result<Option<SymbolId>> {
        for (name_id, membership) in self.names.entries()? {
            validate_name_membership(membership, self.revision(), self.generation)?;
            let stored = self
                .labels
                .symbols()
                .try_resolve(*name_id)
                .map_err(label_error)?;
            if stored == name {
                return Ok(Some(*name_id));
            }
        }
        Ok(None)
    }

    fn resolve_active_pair(
        &self,
        name: &str,
        value: &str,
    ) -> io::Result<Option<(SymbolId, SymbolId)>> {
        let Some(name_id) = self.resolve_active_name(name)? else {
            return Ok(None);
        };
        let lower = LabelValueKey {
            name: name_id,
            value: SymbolId::new(0),
        };
        let upper = LabelValueKey {
            name: name_id,
            value: SymbolId::new(u32::MAX),
        };
        let entries = self.values.range_entries(&lower, &upper)?;
        for (key, membership) in entries {
            validate_value_membership(membership, self.revision(), self.generation)?;
            let stored = self
                .labels
                .symbols()
                .try_resolve(key.value)
                .map_err(label_error)?;
            if stored == value {
                return Ok(Some((name_id, key.value)));
            }
        }
        Ok(None)
    }

    fn present_values(
        &self,
        name: SymbolId,
        present: &[SeriesRef],
    ) -> io::Result<Vec<(SymbolId, &str, Vec<SeriesRef>)>> {
        let mut values = Vec::new();
        let lower = LabelValueKey {
            name,
            value: SymbolId::new(0),
        };
        let upper = LabelValueKey {
            name,
            value: SymbolId::new(u32::MAX),
        };
        let entries = self.values.range_entries(&lower, &upper)?;
        values
            .try_reserve_exact(entries.len())
            .map_err(|error| io::Error::new(io::ErrorKind::OutOfMemory, error))?;
        for (key, membership) in entries {
            validate_value_membership(membership, self.revision(), self.generation)?;
            let posting = self.postings_for_ids(name, key.value, present)?;
            if posting.is_empty() {
                continue;
            }
            let value = self
                .labels
                .symbols()
                .try_resolve(key.value)
                .map_err(label_error)?;
            values.push((key.value, value, posting));
        }
        values.sort_by(|left, right| left.1.cmp(right.1).then(left.0.cmp(&right.0)));
        Ok(values)
    }

    fn postings_for_ids(
        &self,
        name: SymbolId,
        value: SymbolId,
        present: &[SeriesRef],
    ) -> io::Result<Vec<SeriesRef>> {
        let mut posting = Vec::new();
        let lower = PostingKey {
            name,
            value,
            series: SeriesRef::new(0),
        };
        let upper = PostingKey {
            name,
            value,
            series: SeriesRef::new(u32::MAX),
        };
        let entries = self.postings.range_entries(&lower, &upper)?;
        posting
            .try_reserve_exact(entries.len().min(present.len()))
            .map_err(|error| io::Error::new(io::ErrorKind::OutOfMemory, error))?;
        for (key, membership) in entries {
            if membership.row_revision > self.revision()
                || membership.born_generation > self.generation
            {
                return Err(invalid_data(
                    "live posting membership is newer than its pinned catalog cut",
                ));
            }
            if present.binary_search(&key.series).is_ok() {
                posting.push(key.series);
            }
        }
        Ok(posting)
    }

    fn label_value(&self, series: SeriesRef, name: &str) -> io::Result<Option<&str>> {
        let row = self
            .labels
            .try_canonical_labelset_symbol_ids(series)
            .map_err(label_error)?;
        for (name_id, value_id) in row.iter() {
            let stored_name = self
                .labels
                .symbols()
                .try_resolve(name_id)
                .map_err(label_error)?;
            if stored_name == name {
                return self
                    .labels
                    .symbols()
                    .try_resolve(value_id)
                    .map(Some)
                    .map_err(label_error);
            }
        }
        Ok(None)
    }
}

/// Single-writer candidate builder for [`LiveSeriesCatalog`].
///
/// `from_catalog` retains every unchanged map path. `reconcile_sample_store`
/// activates and retires only the set difference against the exact candidate
/// sample root.
#[derive(Clone)]
pub struct LiveSeriesCatalogBuilder {
    labels: Arc<VersionedFlatInternedLabelSetSnapshot>,
    generation: u64,
    active: PersistentMap<SeriesRef, ActiveSeries>,
    postings: PersistentMap<PostingKey, PostingMembership>,
    names: PersistentMap<SymbolId, LabelNameMembership>,
    values: PersistentMap<LabelValueKey, LabelValueMembership>,
}

impl fmt::Debug for LiveSeriesCatalogBuilder {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LiveSeriesCatalogBuilder")
            .field("revision", &self.labels.revision())
            .field("generation", &self.generation)
            .field("active_series", &map_entries(&self.active.root))
            .field("posting_memberships", &map_entries(&self.postings.root))
            .field("active_label_names", &map_entries(&self.names.root))
            .field("active_label_values", &map_entries(&self.values.root))
            .finish()
    }
}

impl LiveSeriesCatalogBuilder {
    pub fn new(
        labels: Arc<VersionedFlatInternedLabelSetSnapshot>,
        candidate_generation: u64,
    ) -> io::Result<LiveSeriesCatalogBuilder> {
        if candidate_generation == 0 {
            return Err(invalid_data(
                "live series catalog generation zero is reserved",
            ));
        }
        validate_revision_rows(&labels, 0, labels.revision())?;
        Ok(Self {
            labels,
            generation: candidate_generation,
            active: PersistentMap::default(),
            postings: PersistentMap::default(),
            names: PersistentMap::default(),
            values: PersistentMap::default(),
        })
    }

    pub fn from_catalog(
        previous: &LiveSeriesCatalog,
        labels: Arc<VersionedFlatInternedLabelSetSnapshot>,
        candidate_generation: u64,
    ) -> io::Result<Self> {
        Self::validate_successor(previous, &labels, candidate_generation)?;
        Ok(Self {
            labels,
            generation: candidate_generation,
            active: previous.active.clone(),
            postings: previous.postings.clone(),
            names: previous.names.clone(),
            values: previous.values.clone(),
        })
    }

    /// Constructs an empty successor after a caller has independently proven
    /// that every sample supplier handed off to the bound sealed inventory.
    ///
    /// The predecessor still supplies lineage, revision, and generation
    /// induction. New append-only label rows are validated even though none is
    /// activated in the empty head catalog.
    pub fn empty_successor(
        previous: &LiveSeriesCatalog,
        labels: Arc<VersionedFlatInternedLabelSetSnapshot>,
        candidate_generation: u64,
    ) -> io::Result<Self> {
        Self::validate_successor(previous, &labels, candidate_generation)?;
        Ok(Self {
            labels,
            generation: candidate_generation,
            active: PersistentMap::default(),
            postings: PersistentMap::default(),
            names: PersistentMap::default(),
            values: PersistentMap::default(),
        })
    }

    fn validate_successor(
        previous: &LiveSeriesCatalog,
        labels: &VersionedFlatInternedLabelSetSnapshot,
        candidate_generation: u64,
    ) -> io::Result<()> {
        if labels.lineage_id() != previous.labels.lineage_id() {
            return Err(invalid_data(
                "live catalog revision came from a different label-store lineage",
            ));
        }
        if labels.revision() < previous.revision() {
            return Err(invalid_data(format!(
                "live catalog revision regressed from {} to {}",
                previous.revision(),
                labels.revision()
            )));
        }
        validate_revision_rows(labels, previous.revision(), labels.revision())?;
        let expected_generation = previous
            .generation
            .checked_add(1)
            .ok_or_else(|| invalid_data("live series catalog generation overflows u64"))?;
        if candidate_generation != expected_generation {
            return Err(invalid_data(format!(
                "live catalog candidate generation {candidate_generation} does not follow \
                 pinned generation {}",
                previous.generation
            )));
        }
        Ok(())
    }

    pub fn reconcile_sample_store(&mut self, samples: &LiveSampleStore) -> io::Result<()> {
        if samples.required_catalog_revision() > self.labels.revision() {
            return Err(invalid_data(format!(
                "candidate samples require catalog revision {}, but snapshot revision is {}",
                samples.required_catalog_revision(),
                self.labels.revision()
            )));
        }
        let target = samples.active_series_refs()?;
        let current_entries = self.active.entries()?;
        let mut current = Vec::new();
        current
            .try_reserve_exact(current_entries.len())
            .map_err(|error| io::Error::new(io::ErrorKind::OutOfMemory, error))?;
        current.extend(current_entries.into_iter().map(|(series, _)| *series));
        let mut candidate = self.clone();
        let mut old = 0usize;
        let mut new = 0usize;
        while old < current.len() || new < target.len() {
            match (current.get(old), target.get(new)) {
                (Some(left), Some(right)) => match left.cmp(right) {
                    Ordering::Less => {
                        candidate.retire(*left)?;
                        old += 1;
                    }
                    Ordering::Greater => {
                        candidate.activate(*right)?;
                        new += 1;
                    }
                    Ordering::Equal => {
                        old += 1;
                        new += 1;
                    }
                },
                (Some(left), None) => {
                    candidate.retire(*left)?;
                    old += 1;
                }
                (None, Some(right)) => {
                    candidate.activate(*right)?;
                    new += 1;
                }
                (None, None) => break,
            }
        }
        *self = candidate;
        Ok(())
    }

    pub fn finish(self) -> io::Result<LiveSeriesCatalog> {
        let catalog = LiveSeriesCatalog {
            labels: self.labels,
            generation: self.generation,
            active: self.active,
            postings: self.postings,
            names: self.names,
            values: self.values,
        };
        catalog.validate_roots()?;
        Ok(catalog)
    }

    fn activate(&mut self, series: SeriesRef) -> io::Result<()> {
        if self.active.get(&series).is_some() {
            return Err(invalid_data(
                "live catalog attempted to activate a duplicate series",
            ));
        }
        let row_revision = u64::from(series.get())
            .checked_add(1)
            .ok_or_else(|| invalid_data("live catalog row revision overflows u64"))?;
        if row_revision > self.labels.revision() {
            return Err(invalid_data(format!(
                "series ref {} is outside catalog revision {}",
                series.get(),
                self.labels.revision()
            )));
        }
        let (series_id, pairs) = validated_row(&self.labels, series)?;
        self.active.insert(
            series,
            ActiveSeries {
                series_id,
                row_revision,
                born_generation: self.generation,
            },
        )?;
        for (name, value) in pairs {
            let name_membership = match self.names.get(&name) {
                Some(existing) => LabelNameMembership {
                    active_series: existing.active_series.checked_add(1).ok_or_else(|| {
                        invalid_data("live label-name active-series count overflows u32")
                    })?,
                    row_revision: existing.row_revision.min(row_revision),
                    born_generation: existing.born_generation,
                },
                None => LabelNameMembership {
                    active_series: 1,
                    row_revision,
                    born_generation: self.generation,
                },
            };
            self.names.insert(name, name_membership)?;
            let posting_key = PostingKey {
                name,
                value,
                series,
            };
            if self.postings.get(&posting_key).is_some() {
                return Err(invalid_data(
                    "live catalog attempted to insert a duplicate posting membership",
                ));
            }
            self.postings.insert(
                posting_key,
                PostingMembership {
                    row_revision,
                    born_generation: self.generation,
                },
            )?;
            let value_key = LabelValueKey { name, value };
            let membership = match self.values.get(&value_key) {
                Some(existing) => LabelValueMembership {
                    active_series: existing.active_series.checked_add(1).ok_or_else(|| {
                        invalid_data("live label-value active-series count overflows u32")
                    })?,
                    row_revision: existing.row_revision.min(row_revision),
                    born_generation: existing.born_generation,
                },
                None => LabelValueMembership {
                    active_series: 1,
                    row_revision,
                    born_generation: self.generation,
                },
            };
            self.values.insert(value_key, membership)?;
        }
        Ok(())
    }

    fn retire(&mut self, series: SeriesRef) -> io::Result<()> {
        if self.active.get(&series).is_none() {
            return Err(invalid_data(
                "live catalog attempted to retire an inactive series",
            ));
        }
        let (_, pairs) = validated_row(&self.labels, series)?;
        if !self.active.remove(&series)? {
            return Err(invalid_data(
                "live catalog active entry disappeared during retirement",
            ));
        }
        for (name, value) in pairs {
            let existing_name = self.names.get(&name).cloned().ok_or_else(|| {
                invalid_data("live catalog name membership disappeared during retirement")
            })?;
            if existing_name.active_series == 1 {
                if !self.names.remove(&name)? {
                    return Err(invalid_data(
                        "live catalog name membership could not be retired",
                    ));
                }
            } else {
                self.names.insert(
                    name,
                    LabelNameMembership {
                        active_series: existing_name.active_series - 1,
                        ..existing_name
                    },
                )?;
            }
            let posting_key = PostingKey {
                name,
                value,
                series,
            };
            if !self.postings.remove(&posting_key)? {
                return Err(invalid_data(
                    "live catalog posting disappeared during retirement",
                ));
            }
            let value_key = LabelValueKey { name, value };
            let existing = self.values.get(&value_key).cloned().ok_or_else(|| {
                invalid_data("live catalog value membership disappeared during retirement")
            })?;
            if existing.active_series == 1 {
                if !self.values.remove(&value_key)? {
                    return Err(invalid_data(
                        "live catalog value membership could not be retired",
                    ));
                }
            } else {
                self.values.insert(
                    value_key,
                    LabelValueMembership {
                        active_series: existing.active_series - 1,
                        ..existing
                    },
                )?;
            }
        }
        Ok(())
    }
}

impl LiveSeriesCatalog {
    fn validate_roots(&self) -> io::Result<()> {
        if let Some(root) = &self.active.root {
            validate_map_node(root)?;
        }
        if let Some(root) = &self.postings.root {
            validate_map_node(root)?;
        }
        if let Some(root) = &self.names.root {
            validate_map_node(root)?;
        }
        if let Some(root) = &self.values.root {
            validate_map_node(root)?;
        }
        Ok(())
    }

    #[cfg(test)]
    fn validate_internal(&self) -> io::Result<()> {
        for (series, active) in self.active.entries()? {
            if active.row_revision != u64::from(series.get()).saturating_add(1)
                || active.row_revision > self.revision()
                || active.born_generation > self.generation
            {
                return Err(invalid_data(
                    "live catalog active-series metadata is outside its pinned cut",
                ));
            }
            let (series_id, pairs) = validated_row(&self.labels, *series)?;
            if series_id != active.series_id {
                return Err(invalid_data(
                    "live catalog stable series ID disagrees with its shared label row",
                ));
            }
            for (name, value) in pairs {
                let posting = self
                    .postings
                    .get(&PostingKey {
                        name,
                        value,
                        series: *series,
                    })
                    .ok_or_else(|| {
                        invalid_data("live catalog active series is missing a posting membership")
                    })?;
                if posting.row_revision != active.row_revision
                    || posting.born_generation > self.generation
                {
                    return Err(invalid_data(
                        "live catalog posting metadata disagrees with its active series",
                    ));
                }
            }
        }
        for (_, value) in self.values.entries()? {
            validate_value_membership(value, self.revision(), self.generation)?;
        }
        for (_, name) in self.names.entries()? {
            validate_name_membership(name, self.revision(), self.generation)?;
        }
        Ok(())
    }
}

fn validated_row(
    labels: &VersionedFlatInternedLabelSetSnapshot,
    series: SeriesRef,
) -> io::Result<(u64, Vec<(SymbolId, SymbolId)>)> {
    let row = labels
        .try_canonical_labelset_symbol_ids(series)
        .map_err(label_error)?;
    let mut pairs = Vec::new();
    pairs
        .try_reserve_exact(row.len())
        .map_err(|error| io::Error::new(io::ErrorKind::OutOfMemory, error))?;
    let mut id_bytes = Vec::new();
    let mut previous_name = None;
    let mut metric_names = 0u8;
    for (name_id, value_id) in row.iter() {
        let name = labels.symbols().try_resolve(name_id).map_err(label_error)?;
        let value = labels
            .symbols()
            .try_resolve(value_id)
            .map_err(label_error)?;
        if previous_name.is_some_and(|previous| previous >= name) {
            return Err(invalid_data(
                "live catalog label row is not strictly canonical by label name",
            ));
        }
        previous_name = Some(name);
        if name == METRIC_NAME_LABEL {
            metric_names = metric_names
                .checked_add(1)
                .ok_or_else(|| invalid_data("live catalog metric-name count overflows u8"))?;
            if normalize_metric_name(value) != value {
                return Err(invalid_data(
                    "live catalog metric identity is not in canonical PromQL form",
                ));
            }
        } else if normalize_label_name(name) != name {
            return Err(invalid_data(
                "live catalog label name is not in canonical PromQL form",
            ));
        }
        id_bytes
            .try_reserve(name.len().saturating_add(value.len()).saturating_add(2))
            .map_err(|error| io::Error::new(io::ErrorKind::OutOfMemory, error))?;
        id_bytes.extend_from_slice(name.as_bytes());
        id_bytes.push(0);
        id_bytes.extend_from_slice(value.as_bytes());
        id_bytes.push(0xff);
        pairs.push((name_id, value_id));
    }
    if metric_names != 1 {
        return Err(invalid_data(
            "live catalog label row must contain exactly one canonical metric identity",
        ));
    }
    Ok((xxhash64(&id_bytes), pairs))
}

fn validate_revision_rows(
    labels: &VersionedFlatInternedLabelSetSnapshot,
    start_revision: u64,
    end_revision: u64,
) -> io::Result<()> {
    if start_revision > end_revision || end_revision > labels.revision() {
        return Err(invalid_data(
            "live catalog row-validation revision range is invalid",
        ));
    }
    for raw in start_revision..end_revision {
        let raw = u32::try_from(raw)
            .map_err(|_| invalid_data("live catalog revision exceeds SeriesRef range"))?;
        validated_row(labels, SeriesRef::new(raw))?;
    }
    Ok(())
}

fn validate_value_membership(
    membership: &LabelValueMembership,
    revision: u64,
    generation: u64,
) -> io::Result<()> {
    if membership.active_series == 0
        || membership.row_revision > revision
        || membership.born_generation > generation
    {
        return Err(invalid_data(
            "live label-value membership is outside its pinned catalog cut",
        ));
    }
    Ok(())
}

fn validate_name_membership(
    membership: &LabelNameMembership,
    revision: u64,
    generation: u64,
) -> io::Result<()> {
    if membership.active_series == 0
        || membership.row_revision > revision
        || membership.born_generation > generation
    {
        return Err(invalid_data(
            "live label-name membership is outside its pinned catalog cut",
        ));
    }
    Ok(())
}

fn validate_sorted_presence(present: &[SeriesRef]) -> io::Result<()> {
    if present.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err(invalid_data(
            "live query time-presence set is not strictly sorted and unique",
        ));
    }
    Ok(())
}

fn label_error(error: VersionedFlatLabelStoreError) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, error)
}

fn try_clone_string(value: &str, region: &'static str) -> io::Result<String> {
    let mut owned = String::new();
    owned.try_reserve_exact(value.len()).map_err(|error| {
        io::Error::new(io::ErrorKind::OutOfMemory, format!("{region}: {error}"))
    })?;
    owned.push_str(value);
    Ok(owned)
}

fn invalid_data(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}

fn map_allocated_bytes<K, V>(entries: u64) -> u64 {
    let allocation_bytes = std::mem::size_of::<MapNode<K, V>>()
        .saturating_add(2usize.saturating_mul(std::mem::size_of::<usize>()));
    entries.saturating_mul(u64::try_from(allocation_bytes).unwrap_or(u64::MAX))
}

#[cfg(test)]
mod tests;
