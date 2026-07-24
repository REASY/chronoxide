use super::compact::*;
use super::*;

/// Query-result labels with the established owned-string layout, the prior
/// shared-string comparator, or query-local compact string IDs.
///
/// Iterate with [`Self::pairs`] or [`Self::visit_pairs`]. Callers that
/// explicitly need owned strings may use [`Self::to_vec`]; the returned copy
/// is caller-owned and is never retained inside the governed query result.
#[derive(Debug, Clone)]
pub struct QueryLabels(pub(super) QueryLabelStorage);

#[derive(Debug, Clone)]
pub(super) enum QueryLabelStorage {
    Owned(Arc<[(String, String)]>),
    Shared(Arc<SharedQueryLabels>),
    Compact(Arc<CompactQueryLabels>),
}

#[derive(Debug)]
pub(super) struct SharedQueryLabels {
    pairs: Arc<[(Arc<str>, Arc<str>)]>,
}

pub struct QueryLabelPairs<'a> {
    inner: QueryLabelPairsInner<'a>,
}

enum QueryLabelPairsInner<'a> {
    Owned(std::slice::Iter<'a, (String, String)>),
    Shared(std::slice::Iter<'a, (Arc<str>, Arc<str>)>),
    Compact {
        pairs: std::slice::Iter<'a, CompactQueryLabelPair>,
        arena: &'a CompactQueryLabelArena,
    },
}

impl<'a> Iterator for QueryLabelPairs<'a> {
    type Item = (&'a str, &'a str);

    fn next(&mut self) -> Option<Self::Item> {
        match &mut self.inner {
            QueryLabelPairsInner::Owned(pairs) => pairs
                .next()
                .map(|(name, value)| (name.as_str(), value.as_str())),
            QueryLabelPairsInner::Shared(pairs) => pairs
                .next()
                .map(|(name, value)| (name.as_ref(), value.as_ref())),
            QueryLabelPairsInner::Compact { pairs, arena } => pairs
                .next()
                .map(|pair| (arena.resolve(pair.name_id), arena.resolve(pair.value_id))),
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        match &self.inner {
            QueryLabelPairsInner::Owned(pairs) => pairs.size_hint(),
            QueryLabelPairsInner::Shared(pairs) => pairs.size_hint(),
            QueryLabelPairsInner::Compact { pairs, .. } => pairs.size_hint(),
        }
    }
}

impl ExactSizeIterator for QueryLabelPairs<'_> {}

impl std::iter::FusedIterator for QueryLabelPairs<'_> {}

impl QueryLabels {
    pub(crate) fn from_vec(labels: Vec<(String, String)>) -> Self {
        Self(QueryLabelStorage::Owned(Arc::from(
            labels.into_boxed_slice(),
        )))
    }

    pub(super) fn from_shared(pairs: Vec<(Arc<str>, Arc<str>)>) -> Self {
        Self(QueryLabelStorage::Shared(Arc::new(SharedQueryLabels {
            pairs: Arc::from(pairs.into_boxed_slice()),
        })))
    }

    pub fn pairs(&self) -> QueryLabelPairs<'_> {
        let inner = match &self.0 {
            QueryLabelStorage::Owned(labels) => QueryLabelPairsInner::Owned(labels.iter()),
            QueryLabelStorage::Shared(labels) => QueryLabelPairsInner::Shared(labels.pairs.iter()),
            QueryLabelStorage::Compact(labels) => QueryLabelPairsInner::Compact {
                pairs: labels.pairs.iter(),
                arena: &labels.arena,
            },
        };
        QueryLabelPairs { inner }
    }

    /// Iterates borrowed label strings without materializing an owned slice.
    pub fn iter(&self) -> QueryLabelPairs<'_> {
        self.pairs()
    }

    /// Visits labels without forcing the owned-string compatibility view.
    pub fn visit_pairs(&self, mut visit: impl FnMut(&str, &str)) {
        for (name, value) in self.pairs() {
            visit(name, value);
        }
    }

    pub fn len(&self) -> usize {
        match &self.0 {
            QueryLabelStorage::Owned(labels) => labels.len(),
            QueryLabelStorage::Shared(labels) => labels.pairs.len(),
            QueryLabelStorage::Compact(labels) => labels.pairs.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub(crate) fn uses_shared_atoms(&self) -> bool {
        matches!(&self.0, QueryLabelStorage::Shared(_))
    }

    pub(crate) fn uses_compact_ids(&self) -> bool {
        matches!(&self.0, QueryLabelStorage::Compact(_))
    }

    pub fn to_vec(&self) -> Vec<(String, String)> {
        match &self.0 {
            QueryLabelStorage::Owned(labels) => labels.to_vec(),
            QueryLabelStorage::Shared(labels) => labels
                .pairs
                .iter()
                .map(|(name, value)| (name.to_string(), value.to_string()))
                .collect(),
            QueryLabelStorage::Compact(_) => self
                .pairs()
                .map(|(name, value)| (name.to_owned(), value.to_owned()))
                .collect(),
        }
    }

    /// Narrows an already matcher-verified selective label set to the names
    /// that the terminal aggregation may expose. Compact labels retain their
    /// query-global IDs and allocate only a new governed pair vector.
    pub(crate) fn try_retain_names(self, names: &[String]) -> io::Result<Self> {
        let selected = |name: &str| {
            names
                .binary_search_by(|candidate| candidate.as_str().cmp(name))
                .is_ok()
        };
        if self.pairs().all(|(name, _)| selected(name)) {
            return Ok(self);
        }

        match self.0 {
            QueryLabelStorage::Owned(labels) => {
                let selected_count = labels.iter().filter(|(name, _)| selected(name)).count();
                let mut output = Vec::new();
                output.try_reserve_exact(selected_count).map_err(|_| {
                    io::Error::new(
                        io::ErrorKind::OutOfMemory,
                        "projected owned query-label allocation failed",
                    )
                })?;
                output.extend(labels.iter().filter(|(name, _)| selected(name)).cloned());
                Ok(Self::from_vec(output))
            }
            QueryLabelStorage::Shared(labels) => {
                let selected_count = labels
                    .pairs
                    .iter()
                    .filter(|(name, _)| selected(name))
                    .count();
                let mut output = Vec::new();
                output.try_reserve_exact(selected_count).map_err(|_| {
                    io::Error::new(
                        io::ErrorKind::OutOfMemory,
                        "projected shared query-label allocation failed",
                    )
                })?;
                output.extend(
                    labels
                        .pairs
                        .iter()
                        .filter(|(name, _)| selected(name))
                        .cloned(),
                );
                Ok(Self::from_shared(output))
            }
            QueryLabelStorage::Compact(labels) => {
                let selected_count = labels
                    .pairs
                    .iter()
                    .filter(|pair| selected(labels.arena.resolve(pair.name_id)))
                    .count();
                let pair_count = u64::try_from(selected_count).map_err(|_| {
                    io::Error::new(
                        io::ErrorKind::OutOfMemory,
                        "projected compact query-label pair count exceeds u64",
                    )
                })?;
                let label_block_bytes = compact_query_label_block_bytes(pair_count)?;
                labels
                    .arena
                    .reserve_category(CompactQueryLabelChargeCategory::Pairs, label_block_bytes)?;
                let mut output = Vec::new();
                if output.try_reserve_exact(selected_count).is_err() {
                    labels.arena.release_category(
                        CompactQueryLabelChargeCategory::Pairs,
                        label_block_bytes,
                    );
                    return Err(io::Error::new(
                        io::ErrorKind::OutOfMemory,
                        "projected compact query-label allocation failed",
                    ));
                }
                output.extend(
                    labels
                        .pairs
                        .iter()
                        .filter(|pair| selected(labels.arena.resolve(pair.name_id)))
                        .copied(),
                );
                {
                    let mut state = labels
                        .arena
                        .state
                        .lock()
                        .unwrap_or_else(|error| error.into_inner());
                    state.label_sets = state.label_sets.saturating_add(1);
                    state.label_pairs = state.label_pairs.saturating_add(pair_count);
                }
                Ok(labels
                    .arena
                    .labels_from_pairs_reserved(output, label_block_bytes))
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn ptr_eq(&self, other: &Self) -> bool {
        match (&self.0, &other.0) {
            (QueryLabelStorage::Owned(left), QueryLabelStorage::Owned(right)) => {
                Arc::ptr_eq(left, right)
            }
            (QueryLabelStorage::Shared(left), QueryLabelStorage::Shared(right)) => {
                Arc::ptr_eq(left, right)
            }
            (QueryLabelStorage::Compact(left), QueryLabelStorage::Compact(right)) => {
                Arc::ptr_eq(left, right)
            }
            _ => false,
        }
    }

    #[cfg(test)]
    pub(crate) fn shared_atom_ptrs(&self) -> Option<Vec<(*const str, *const str)>> {
        match &self.0 {
            QueryLabelStorage::Owned(_) => None,
            QueryLabelStorage::Shared(labels) => Some(
                labels
                    .pairs
                    .iter()
                    .map(|(name, value)| (Arc::as_ptr(name), Arc::as_ptr(value)))
                    .collect(),
            ),
            QueryLabelStorage::Compact(_) => None,
        }
    }

    #[cfg(test)]
    pub(crate) fn owned_compatibility_materialized(&self) -> bool {
        false
    }

    #[cfg(test)]
    pub(crate) fn compact_charge_categories_for_test(&self) -> Option<(u64, u64, u64, u64, u64)> {
        let QueryLabelStorage::Compact(labels) = &self.0 else {
            return None;
        };
        let snapshot = labels.arena.snapshot();
        Some((
            snapshot.current_bytes,
            snapshot.atom_bytes,
            snapshot.pair_bytes,
            snapshot.hash_directory_bytes,
            snapshot.translation_bytes,
        ))
    }

    /// Test diagnostic for consumers that must not force the owned-string
    /// compatibility view while handling shared query-label atoms.
    #[doc(hidden)]
    pub fn shared_atoms_compatibility_view_materialized_for_test(&self) -> Option<bool> {
        match &self.0 {
            QueryLabelStorage::Owned(_) => None,
            QueryLabelStorage::Shared(_) => Some(false),
            QueryLabelStorage::Compact(_) => None,
        }
    }

    #[doc(hidden)]
    pub fn compact_ids_compatibility_view_materialized_for_test(&self) -> Option<bool> {
        match &self.0 {
            QueryLabelStorage::Compact(_) => Some(false),
            QueryLabelStorage::Owned(_) | QueryLabelStorage::Shared(_) => None,
        }
    }
}

impl PartialEq for QueryLabels {
    fn eq(&self, other: &Self) -> bool {
        if let (QueryLabelStorage::Compact(left), QueryLabelStorage::Compact(right)) =
            (&self.0, &other.0)
            && Arc::ptr_eq(&left.arena, &right.arena)
        {
            return left.pairs == right.pairs;
        }
        self.pairs().eq(other.pairs())
    }
}

impl Eq for QueryLabels {}

impl PartialOrd for QueryLabels {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for QueryLabels {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.pairs().cmp(other.pairs())
    }
}

impl PartialEq<Vec<(String, String)>> for QueryLabels {
    fn eq(&self, other: &Vec<(String, String)>) -> bool {
        self.pairs().eq(other
            .iter()
            .map(|(name, value)| (name.as_str(), value.as_str())))
    }
}

impl PartialEq<QueryLabels> for Vec<(String, String)> {
    fn eq(&self, other: &QueryLabels) -> bool {
        other == self
    }
}

pub(crate) fn shared_query_labels(labels: Vec<(String, String)>) -> QueryLabels {
    QueryLabels::from_vec(labels)
}

pub(crate) fn query_labels_series_id(labels: &QueryLabels) -> u64 {
    let mut hash = XxHash64::default();
    for (name, value) in labels.pairs() {
        hash.update(name.as_bytes());
        hash.update(&[0]);
        hash.update(value.as_bytes());
        hash.update(&[0xff]);
    }
    hash.finish()
}
