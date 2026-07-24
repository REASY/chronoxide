use super::compact::*;
use super::model::{QueryLabelStorage, QueryLabels};
use super::*;

/// Runtime-selectable source-label representation for one query session.
/// `OwnedStrings` is the exact established ownership comparator; it is never
/// selected as corruption recovery.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum QueryLabelStoragePolicy {
    SharedAtoms,
    CompactIds,
    #[default]
    OwnedStrings,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct QueryLabelStorageStats {
    pub label_sets: u64,
    pub atom_lookups: u64,
    pub atom_hits: u64,
    pub atom_misses: u64,
    pub unique_content_bytes: u64,
    pub compact_label_sets: u64,
    pub compact_pairs: u64,
    pub compact_atom_lookups: u64,
    pub compact_atom_hits: u64,
    pub compact_atom_misses: u64,
    pub compact_source_symbol_translations: u64,
    pub compact_source_symbol_translation_hits: u64,
    pub compact_source_symbol_translation_misses: u64,
    pub compact_unique_strings: u64,
    pub compact_unique_content_bytes: u64,
    pub compact_arena_budget_bytes: u64,
    pub compact_arena_current_bytes: u64,
    pub compact_arena_peak_bytes: u64,
    pub compact_atom_bytes: u64,
    pub compact_pair_bytes: u64,
    pub compact_hash_directory_bytes: u64,
    pub compact_translation_bytes: u64,
    pub compact_retained_bytes: u64,
    pub compact_arena_admission_refusals: u64,
    pub compact_compatibility_materializations: u64,
}

impl QueryLabelStorageStats {
    pub fn delta_since(self, earlier: Self) -> Self {
        Self {
            label_sets: self.label_sets.saturating_sub(earlier.label_sets),
            atom_lookups: self.atom_lookups.saturating_sub(earlier.atom_lookups),
            atom_hits: self.atom_hits.saturating_sub(earlier.atom_hits),
            atom_misses: self.atom_misses.saturating_sub(earlier.atom_misses),
            unique_content_bytes: self
                .unique_content_bytes
                .saturating_sub(earlier.unique_content_bytes),
            compact_label_sets: self
                .compact_label_sets
                .saturating_sub(earlier.compact_label_sets),
            compact_pairs: self.compact_pairs.saturating_sub(earlier.compact_pairs),
            compact_atom_lookups: self
                .compact_atom_lookups
                .saturating_sub(earlier.compact_atom_lookups),
            compact_atom_hits: self
                .compact_atom_hits
                .saturating_sub(earlier.compact_atom_hits),
            compact_atom_misses: self
                .compact_atom_misses
                .saturating_sub(earlier.compact_atom_misses),
            compact_source_symbol_translations: self
                .compact_source_symbol_translations
                .saturating_sub(earlier.compact_source_symbol_translations),
            compact_source_symbol_translation_hits: self
                .compact_source_symbol_translation_hits
                .saturating_sub(earlier.compact_source_symbol_translation_hits),
            compact_source_symbol_translation_misses: self
                .compact_source_symbol_translation_misses
                .saturating_sub(earlier.compact_source_symbol_translation_misses),
            compact_unique_strings: self
                .compact_unique_strings
                .saturating_sub(earlier.compact_unique_strings),
            compact_unique_content_bytes: self
                .compact_unique_content_bytes
                .saturating_sub(earlier.compact_unique_content_bytes),
            compact_arena_budget_bytes: self.compact_arena_budget_bytes,
            compact_arena_current_bytes: self.compact_arena_current_bytes,
            compact_arena_peak_bytes: self.compact_arena_peak_bytes,
            compact_atom_bytes: self.compact_atom_bytes,
            compact_pair_bytes: self.compact_pair_bytes,
            compact_hash_directory_bytes: self.compact_hash_directory_bytes,
            compact_translation_bytes: self.compact_translation_bytes,
            compact_retained_bytes: self.compact_retained_bytes,
            compact_arena_admission_refusals: self
                .compact_arena_admission_refusals
                .saturating_sub(earlier.compact_arena_admission_refusals),
            compact_compatibility_materializations: self
                .compact_compatibility_materializations
                .saturating_sub(earlier.compact_compatibility_materializations),
        }
    }
}

#[derive(Debug)]
pub(in crate::storage::segment) struct QueryLabelInterner {
    policy: QueryLabelStoragePolicy,
    atoms: HashSet<Arc<str>>,
    stats: QueryLabelStorageStats,
    compact_arena_max_bytes: u64,
    compact_arena: Option<Arc<CompactQueryLabelArena>>,
    compact_translations: Vec<SegmentAtomTranslations>,
    // Keep this after the Vec: its modeled capacity charge is released only
    // after the translation elements and outer Vec allocation are dropped.
    compact_translation_list_charge: Option<CompactQueryLabelChargeGuard>,
}

impl Default for QueryLabelInterner {
    fn default() -> Self {
        Self {
            policy: QueryLabelStoragePolicy::default(),
            atoms: HashSet::default(),
            stats: QueryLabelStorageStats::default(),
            compact_arena_max_bytes: DEFAULT_QUERY_LABEL_ARENA_MAX_BYTES,
            compact_arena: None,
            compact_translations: Vec::new(),
            compact_translation_list_charge: None,
        }
    }
}

impl QueryLabelInterner {
    pub(in crate::storage::segment) fn set_policy(&mut self, policy: QueryLabelStoragePolicy) {
        self.policy = policy;
    }

    pub(in crate::storage::segment) fn set_compact_arena_max_bytes(
        &mut self,
        max_bytes: u64,
    ) -> io::Result<()> {
        if max_bytes > MAX_QUERY_LABEL_ARENA_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "query label arena budget {max_bytes} exceeds maximum {MAX_QUERY_LABEL_ARENA_BYTES}"
                ),
            ));
        }
        if self.compact_arena.is_some() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "query label arena budget must be set before compact labels are created",
            ));
        }
        self.compact_arena_max_bytes = max_bytes;
        Ok(())
    }

    pub(in crate::storage::segment) fn policy(&self) -> QueryLabelStoragePolicy {
        self.policy
    }

    pub(in crate::storage::segment) fn stats(&self) -> QueryLabelStorageStats {
        let compact = self
            .compact_arena
            .as_deref()
            .map(CompactQueryLabelArena::snapshot)
            .unwrap_or_else(|| CompactQueryLabelArenaSnapshot {
                budget_bytes: if self.policy == QueryLabelStoragePolicy::CompactIds {
                    self.compact_arena_max_bytes
                } else {
                    0
                },
                admission_refusals: self.stats.compact_arena_admission_refusals,
                ..CompactQueryLabelArenaSnapshot::default()
            });
        QueryLabelStorageStats {
            compact_label_sets: compact.label_sets,
            compact_pairs: compact.label_pairs,
            compact_atom_lookups: compact.lookups,
            compact_atom_hits: compact.hits,
            compact_atom_misses: compact.misses,
            compact_unique_strings: compact.unique_strings,
            compact_unique_content_bytes: compact.unique_content_bytes,
            compact_arena_budget_bytes: compact.budget_bytes,
            compact_arena_current_bytes: compact.current_bytes,
            compact_arena_peak_bytes: compact.peak_bytes,
            compact_atom_bytes: compact.atom_bytes,
            compact_pair_bytes: compact.pair_bytes,
            compact_hash_directory_bytes: compact.hash_directory_bytes,
            compact_translation_bytes: compact.translation_bytes,
            compact_retained_bytes: compact.current_bytes,
            compact_arena_admission_refusals: compact.admission_refusals,
            compact_compatibility_materializations: compact.compatibility_materializations,
            ..self.stats
        }
    }

    pub(in crate::storage::segment) fn try_intern_labels(
        &mut self,
        labels: Vec<(String, String)>,
    ) -> io::Result<QueryLabels> {
        self.stats.label_sets = self.stats.label_sets.saturating_add(1);
        if self.policy == QueryLabelStoragePolicy::OwnedStrings {
            return Ok(QueryLabels::from_vec(labels));
        }
        if self.policy == QueryLabelStoragePolicy::CompactIds {
            let arena = self.compact_arena()?;
            return arena.intern_pairs(labels);
        }

        let pairs = labels
            .into_iter()
            .map(|(name, value)| (self.intern(name), self.intern(value)))
            .collect();
        Ok(QueryLabels::from_shared(pairs))
    }

    /// Rewrites the canonical metric name for a typed PromQL scalar
    /// projection. Compact inputs retain every existing ID and intern the
    /// derived metric atom once per `(metric_name_id, suffix)` pair.
    pub(in crate::storage::segment) fn try_project_metric_suffix_labels(
        &mut self,
        labels: &QueryLabels,
        suffix: &'static str,
    ) -> io::Result<QueryLabels> {
        if self.policy == QueryLabelStoragePolicy::CompactIds
            && let QueryLabelStorage::Compact(compact) = &labels.0
        {
            self.stats.label_sets = self.stats.label_sets.saturating_add(1);
            let arena = self.compact_arena()?;
            return arena.project_metric_suffix(compact, suffix);
        }

        let mut projected = labels.to_vec();
        let mut metric_seen = false;
        for (name, value) in &mut projected {
            if name == METRIC_NAME_LABEL {
                value.push_str(suffix);
                metric_seen = true;
                break;
            }
        }
        if !metric_seen {
            projected.push((METRIC_NAME_LABEL.to_string(), suffix.to_string()));
            projected.sort_by(|left, right| left.0.cmp(&right.0));
        }
        self.try_intern_labels(projected)
    }

    /// Translates one fully verified schema-7/8 source row into compact
    /// query-global IDs. A generation-local source symbol is hashed and
    /// compared only on its first translation miss; all later occurrences are
    /// direct paged-table lookups.
    pub(in crate::storage::segment) fn try_intern_encoded_labels(
        &mut self,
        labels: SegmentEncodedLabels<'_>,
    ) -> io::Result<QueryLabels> {
        if self.policy != QueryLabelStoragePolicy::CompactIds {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "encoded source labels require the compact-ids query label policy",
            ));
        }
        self.stats.label_sets = self.stats.label_sets.saturating_add(1);
        let arena = self.compact_arena()?;
        let pair_count = u64::try_from(labels.pairs().len()).map_err(|_| {
            io::Error::new(
                io::ErrorKind::OutOfMemory,
                "compact source-label pair count exceeds u64",
            )
        })?;
        let label_block_bytes = compact_query_label_block_bytes(pair_count)?;
        arena.reserve_category(CompactQueryLabelChargeCategory::Pairs, label_block_bytes)?;
        let mut pairs = Vec::new();
        if pairs.try_reserve_exact(labels.pairs().len()).is_err() {
            arena.release_category(CompactQueryLabelChargeCategory::Pairs, label_block_bytes);
            return Err(io::Error::new(
                io::ErrorKind::OutOfMemory,
                "compact source-label pair allocation failed",
            ));
        }

        let translation_index = match self
            .compact_translations
            .iter()
            .position(|translation| translation.provenance.same_generation(labels.provenance()))
        {
            Some(index) => {
                if self.compact_translations[index].symbol_count != labels.symbol_count() {
                    arena.release_category(
                        CompactQueryLabelChargeCategory::Pairs,
                        label_block_bytes,
                    );
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "same-generation source symbol count changed during query",
                    ));
                }
                index
            }
            None => {
                let translation = match SegmentAtomTranslations::new(
                    labels.provenance().clone(),
                    labels.symbol_count(),
                    Arc::clone(&arena),
                ) {
                    Ok(translation) => translation,
                    Err(error) => {
                        arena.release_category(
                            CompactQueryLabelChargeCategory::Pairs,
                            label_block_bytes,
                        );
                        return Err(error);
                    }
                };
                if let Err(error) = arena.reserve_category(
                    CompactQueryLabelChargeCategory::Translations,
                    COMPACT_QUERY_LABEL_TRANSLATION_LIST_ENTRY_BYTES,
                ) {
                    drop(translation);
                    arena.release_category(
                        CompactQueryLabelChargeCategory::Pairs,
                        label_block_bytes,
                    );
                    return Err(error);
                }
                if self.compact_translations.try_reserve(1).is_err() {
                    arena.release_category(
                        CompactQueryLabelChargeCategory::Translations,
                        COMPACT_QUERY_LABEL_TRANSLATION_LIST_ENTRY_BYTES,
                    );
                    drop(translation);
                    arena.release_category(
                        CompactQueryLabelChargeCategory::Pairs,
                        label_block_bytes,
                    );
                    return Err(io::Error::new(
                        io::ErrorKind::OutOfMemory,
                        "segment translation-table list allocation failed",
                    ));
                }
                self.compact_translations.push(translation);
                if let Some(charge) = &self.compact_translation_list_charge {
                    charge.add_reserved(COMPACT_QUERY_LABEL_TRANSLATION_LIST_ENTRY_BYTES);
                } else {
                    self.compact_translation_list_charge =
                        Some(CompactQueryLabelChargeGuard::from_reserved(
                            Arc::clone(&arena),
                            CompactQueryLabelChargeCategory::Translations,
                            COMPACT_QUERY_LABEL_TRANSLATION_LIST_ENTRY_BYTES,
                        ));
                }
                self.compact_translations.len() - 1
            }
        };

        for &(source_name_id, source_value_id) in labels.pairs() {
            let name_id = match self.translate_source_symbol(
                translation_index,
                source_name_id,
                labels,
                &arena,
            ) {
                Ok(id) => id,
                Err(error) => {
                    arena.release_category(
                        CompactQueryLabelChargeCategory::Pairs,
                        label_block_bytes,
                    );
                    return Err(error);
                }
            };
            let value_id = match self.translate_source_symbol(
                translation_index,
                source_value_id,
                labels,
                &arena,
            ) {
                Ok(id) => id,
                Err(error) => {
                    arena.release_category(
                        CompactQueryLabelChargeCategory::Pairs,
                        label_block_bytes,
                    );
                    return Err(error);
                }
            };
            pairs.push(CompactQueryLabelPair { name_id, value_id });
        }
        {
            let mut state = arena
                .state
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            state.label_sets = state.label_sets.saturating_add(1);
            state.label_pairs = state.label_pairs.saturating_add(pair_count);
        }
        Ok(arena.labels_from_pairs_reserved(pairs, label_block_bytes))
    }

    fn translate_source_symbol(
        &mut self,
        translation_index: usize,
        source_id: u32,
        labels: SegmentEncodedLabels<'_>,
        arena: &Arc<CompactQueryLabelArena>,
    ) -> io::Result<u32> {
        self.stats.compact_source_symbol_translations = self
            .stats
            .compact_source_symbol_translations
            .saturating_add(1);
        if let Some(query_id) = self.compact_translations[translation_index].lookup(source_id)? {
            self.stats.compact_source_symbol_translation_hits = self
                .stats
                .compact_source_symbol_translation_hits
                .saturating_add(1);
            return Ok(query_id);
        }
        self.stats.compact_source_symbol_translation_misses = self
            .stats
            .compact_source_symbol_translation_misses
            .saturating_add(1);
        let mut query_id = None;
        labels.visit_required_symbol(source_id, |resolved| {
            query_id = Some(arena.intern_borrowed(resolved)?);
            Ok(())
        })?;
        let query_id = query_id.ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "required source symbol resolver returned no value",
            )
        })?;
        self.compact_translations[translation_index].publish(source_id, query_id)?;
        Ok(query_id)
    }

    #[cfg(test)]
    pub(in crate::storage::segment) fn intern_labels(
        &mut self,
        labels: Vec<(String, String)>,
    ) -> QueryLabels {
        self.try_intern_labels(labels)
            .expect("legacy query label policies are infallible")
    }

    pub(in crate::storage::segment) fn intern_result_labels(
        &mut self,
        results: &mut [SegmentQueryResult],
    ) -> io::Result<()> {
        if self.policy == QueryLabelStoragePolicy::OwnedStrings {
            return Ok(());
        }
        for result in results {
            if result.labels.uses_shared_atoms() || result.labels.uses_compact_ids() {
                continue;
            }
            result.labels = self.try_intern_labels(result.labels.to_vec())?;
        }
        Ok(())
    }

    fn intern(&mut self, value: String) -> Arc<str> {
        self.stats.atom_lookups = self.stats.atom_lookups.saturating_add(1);
        let content_bytes = u64::try_from(value.len()).unwrap_or(u64::MAX);
        let (atom, inserted) = intern_query_label_atom(&mut self.atoms, value);
        if !inserted {
            self.stats.atom_hits = self.stats.atom_hits.saturating_add(1);
            return atom;
        }

        self.stats.atom_misses = self.stats.atom_misses.saturating_add(1);
        self.stats.unique_content_bytes = self
            .stats
            .unique_content_bytes
            .saturating_add(content_bytes);
        atom
    }

    fn compact_arena(&mut self) -> io::Result<Arc<CompactQueryLabelArena>> {
        if let Some(arena) = &self.compact_arena {
            return Ok(Arc::clone(arena));
        }
        let arena = match CompactQueryLabelArena::new(self.compact_arena_max_bytes) {
            Ok(arena) => Arc::new(arena),
            Err(error) => {
                self.stats.compact_arena_admission_refusals = self
                    .stats
                    .compact_arena_admission_refusals
                    .saturating_add(1);
                return Err(error);
            }
        };
        self.compact_arena = Some(Arc::clone(&arena));
        Ok(arena)
    }
}

pub(in crate::storage::segment::query_types) fn intern_query_label_atom<S>(
    atoms: &mut HashSet<Arc<str>, S>,
    value: String,
) -> (Arc<str>, bool)
where
    S: std::hash::BuildHasher,
{
    if let Some(existing) = atoms.get(value.as_str()) {
        return (Arc::clone(existing), false);
    }
    let atom: Arc<str> = Arc::from(value.into_boxed_str());
    atoms.insert(Arc::clone(&atom));
    (atom, true)
}
