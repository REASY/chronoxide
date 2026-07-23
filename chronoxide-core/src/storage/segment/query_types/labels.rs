use super::super::metadata_facade::SegmentEncodedLabels;
use super::super::{
    Arc, AtomicU64, HashMap, HashSet, METRIC_NAME_LABEL, Mutex, OnceLock, Ordering, XxHash64, io,
};
use super::result::SegmentQueryResult;
use crate::storage::metadata_runtime::SegmentGenerationProvenance;
use smallvec::SmallVec;

/// Query-result labels with the established owned-string layout, the prior
/// shared-string comparator, or query-local compact string IDs.
///
/// Iterate with [`Self::pairs`] or [`Self::visit_pairs`]. Callers that
/// explicitly need owned strings may use [`Self::to_vec`]; the returned copy
/// is caller-owned and is never retained inside the governed query result.
#[derive(Debug, Clone)]
pub struct QueryLabels(QueryLabelStorage);

#[derive(Debug, Clone)]
enum QueryLabelStorage {
    Owned(Arc<[(String, String)]>),
    Shared(Arc<SharedQueryLabels>),
    Compact(Arc<CompactQueryLabels>),
}

#[derive(Debug)]
struct SharedQueryLabels {
    pairs: Arc<[(Arc<str>, Arc<str>)]>,
}

const COMPACT_QUERY_LABEL_PAIR_BYTES: u64 = std::mem::size_of::<CompactQueryLabelPair>() as u64;
// `std::collections::HashMap` does not expose its raw table layout. Charge a
// deliberately conservative per-admission envelope (two full key/value
// entries plus control/allocation slack), and also retain a fixed first-table
// reserve below. This is a portable admission model, not allocator usable_size
// or a claim about implementation-specific HashMap capacity growth.
const COMPACT_QUERY_LABEL_HASH_ENTRY_BYTES: u64 =
    (2 * std::mem::size_of::<(u64, SmallVec<[u32; 1]>)>() + 32) as u64;
const COMPACT_QUERY_LABEL_DERIVED_ENTRY_BYTES: u64 =
    (2 * std::mem::size_of::<((u32, &'static str), u32)>() + 32) as u64;
pub(super) const COMPACT_QUERY_LABEL_HASH_TABLE_FIXED_RESERVE_BYTES: u64 = 512;
pub(super) const COMPACT_QUERY_LABEL_ATOM_CHUNK_LEN: usize = 4096;
const COMPACT_QUERY_LABEL_TRANSLATION_PAGE_LEN: usize = 4096;
const COMPACT_QUERY_LABEL_UNTRANSLATED: u32 = u32::MAX;

pub const DEFAULT_QUERY_LABEL_ARENA_MAX_BYTES: u64 = 512 * 1024 * 1024;
pub const MAX_QUERY_LABEL_ARENA_BYTES: u64 = 8 * 1024 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct CompactQueryLabelPair {
    name_id: u32,
    value_id: u32,
}

#[derive(Debug, Clone, Copy)]
enum CompactQueryLabelChargeCategory {
    Atoms,
    Pairs,
    HashDirectory,
    Translations,
}

/// Releases an admitted modeled charge only after payload fields declared
/// before this guard have been dropped.
#[derive(Debug)]
struct CompactQueryLabelChargeGuard {
    arena: Arc<CompactQueryLabelArena>,
    category: CompactQueryLabelChargeCategory,
    bytes: AtomicU64,
}

impl CompactQueryLabelChargeGuard {
    fn from_reserved(
        arena: Arc<CompactQueryLabelArena>,
        category: CompactQueryLabelChargeCategory,
        bytes: u64,
    ) -> Self {
        Self {
            arena,
            category,
            bytes: AtomicU64::new(bytes),
        }
    }

    fn add_reserved(&self, bytes: u64) {
        self.bytes
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                current.checked_add(bytes)
            })
            .expect("compact query-label charge guard overflow");
    }
}

impl Drop for CompactQueryLabelChargeGuard {
    fn drop(&mut self) {
        self.arena
            .release_category(self.category, self.bytes.load(Ordering::Relaxed));
    }
}

#[derive(Debug)]
struct CompactQueryLabels {
    pairs: Box<[CompactQueryLabelPair]>,
    arena: Arc<CompactQueryLabelArena>,
    // Keep this last: fields are dropped in declaration order, so the pair
    // allocation is gone before its modeled charge becomes reusable.
    _charge_guard: CompactQueryLabelChargeGuard,
}

const fn align_up_saturating(value: usize, alignment: usize) -> usize {
    value.saturating_add(alignment.saturating_sub(1)) / alignment * alignment
}

pub(super) const fn modeled_arc_allocation_bytes<T>() -> u64 {
    let header_bytes = 2usize.saturating_mul(std::mem::size_of::<usize>());
    let value_alignment = std::mem::align_of::<T>();
    let allocation_alignment = if value_alignment > std::mem::align_of::<usize>() {
        value_alignment
    } else {
        std::mem::align_of::<usize>()
    };
    let value_offset = align_up_saturating(header_bytes, value_alignment);
    align_up_saturating(
        value_offset.saturating_add(std::mem::size_of::<T>()),
        allocation_alignment,
    ) as u64
}

pub(super) fn modeled_arc_str_allocation_bytes(content_bytes: u64) -> io::Result<u64> {
    let header_bytes =
        u64::try_from(2usize.saturating_mul(std::mem::size_of::<usize>())).map_err(|_| {
            io::Error::new(
                io::ErrorKind::OutOfMemory,
                "compact query label atom header charge exceeds u64",
            )
        })?;
    let alignment = u64::try_from(std::mem::align_of::<usize>()).expect("usize alignment fits u64");
    let unaligned = header_bytes.checked_add(content_bytes).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::OutOfMemory,
            "compact query label atom charge overflows",
        )
    })?;
    unaligned
        .checked_add(alignment - 1)
        .map(|bytes| bytes / alignment * alignment)
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::OutOfMemory,
                "compact query label aligned atom charge overflows",
            )
        })
}

pub(super) const COMPACT_QUERY_LABEL_OBJECT_BYTES: u64 =
    modeled_arc_allocation_bytes::<CompactQueryLabels>();

fn compact_query_label_block_bytes(pair_count: u64) -> io::Result<u64> {
    pair_count
        .checked_mul(COMPACT_QUERY_LABEL_PAIR_BYTES)
        .and_then(|bytes| bytes.checked_add(COMPACT_QUERY_LABEL_OBJECT_BYTES))
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::OutOfMemory,
                "compact query label-block charge overflows",
            )
        })
}

pub(super) type CompactQueryLabelAtomChunk = Box<[OnceLock<Arc<str>>]>;
type CompactQueryLabelAtomDirectory = Box<[OnceLock<CompactQueryLabelAtomChunk>]>;

#[derive(Debug)]
pub(super) struct CompactQueryLabelArena {
    max_bytes: u64,
    current_bytes: AtomicU64,
    peak_bytes: AtomicU64,
    atom_bytes: AtomicU64,
    pair_bytes: AtomicU64,
    hash_directory_bytes: AtomicU64,
    translation_bytes: AtomicU64,
    admission_refusals: AtomicU64,
    compatibility_materializations: AtomicU64,
    pub(super) atom_chunks: CompactQueryLabelAtomDirectory,
    hash_builder: ahash::RandomState,
    pub(super) state: Mutex<CompactQueryLabelArenaState>,
}

#[derive(Debug, Default)]
pub(super) struct CompactQueryLabelArenaState {
    hash_buckets: HashMap<u64, SmallVec<[u32; 1]>>,
    derived_metric_atoms: HashMap<(u32, &'static str), u32>,
    next_atom_id: u32,
    pub(super) lookups: u64,
    pub(super) hits: u64,
    pub(super) misses: u64,
    unique_content_bytes: u64,
    label_sets: u64,
    label_pairs: u64,
}

#[derive(Debug, Clone, Copy, Default)]
pub(super) struct CompactQueryLabelArenaSnapshot {
    lookups: u64,
    hits: u64,
    misses: u64,
    unique_content_bytes: u64,
    label_sets: u64,
    label_pairs: u64,
    pub(super) current_bytes: u64,
    pub(super) peak_bytes: u64,
    admission_refusals: u64,
    compatibility_materializations: u64,
    unique_strings: u64,
    budget_bytes: u64,
    pub(super) atom_bytes: u64,
    pub(super) pair_bytes: u64,
    pub(super) hash_directory_bytes: u64,
    pub(super) translation_bytes: u64,
}

impl CompactQueryLabelArena {
    pub(super) fn new(max_bytes: u64) -> io::Result<Self> {
        if max_bytes > MAX_QUERY_LABEL_ARENA_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "query label arena budget {max_bytes} exceeds maximum {MAX_QUERY_LABEL_ARENA_BYTES}"
                ),
            ));
        }
        let minimum_atom_bytes = COMPACT_QUERY_LABEL_HASH_ENTRY_BYTES.max(1);
        let max_atoms_from_budget = max_bytes
            .checked_div(minimum_atom_bytes)
            .unwrap_or(0)
            .saturating_add(1)
            .min(u64::from(u32::MAX));
        let max_atoms = usize::try_from(max_atoms_from_budget).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "query label arena atom limit exceeds usize",
            )
        })?;
        let chunk_count = max_atoms.saturating_add(COMPACT_QUERY_LABEL_ATOM_CHUNK_LEN - 1)
            / COMPACT_QUERY_LABEL_ATOM_CHUNK_LEN;
        let directory_bytes = u64::try_from(
            chunk_count.saturating_mul(std::mem::size_of::<OnceLock<CompactQueryLabelAtomChunk>>()),
        )
        .map_err(|_| {
            io::Error::new(
                io::ErrorKind::OutOfMemory,
                "query label arena chunk-directory charge exceeds u64",
            )
        })?;
        let arena_base_bytes = modeled_arc_allocation_bytes::<CompactQueryLabelArena>()
            .checked_add(directory_bytes)
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::OutOfMemory,
                    "query label arena base charge overflows",
                )
            })?;
        let initial_charge = arena_base_bytes
            .checked_add(COMPACT_QUERY_LABEL_HASH_TABLE_FIXED_RESERVE_BYTES)
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::OutOfMemory,
                    "query label arena initial charge overflows",
                )
            })?;
        if initial_charge > max_bytes {
            return Err(io::Error::new(
                io::ErrorKind::OutOfMemory,
                format!(
                    "query label arena budget of {max_bytes} bytes cannot admit its {initial_charge}-byte base allocation"
                ),
            ));
        }
        let mut chunks = Vec::new();
        chunks.try_reserve_exact(chunk_count).map_err(|_| {
            io::Error::new(
                io::ErrorKind::OutOfMemory,
                "query label arena chunk-directory allocation failed",
            )
        })?;
        chunks.resize_with(chunk_count, OnceLock::new);
        Ok(Self {
            max_bytes,
            current_bytes: AtomicU64::new(initial_charge),
            peak_bytes: AtomicU64::new(initial_charge),
            // The atom-storage category includes the arena Arc/root and its
            // fixed atom-directory allocation in addition to admitted atom
            // chunks and Arc<str> payloads. Charges model requested live
            // allocation bytes; allocator metadata and size-class slack are
            // intentionally measured separately through process RSS.
            atom_bytes: AtomicU64::new(arena_base_bytes),
            pair_bytes: AtomicU64::new(0),
            hash_directory_bytes: AtomicU64::new(
                COMPACT_QUERY_LABEL_HASH_TABLE_FIXED_RESERVE_BYTES,
            ),
            translation_bytes: AtomicU64::new(0),
            admission_refusals: AtomicU64::new(0),
            compatibility_materializations: AtomicU64::new(0),
            atom_chunks: chunks.into_boxed_slice(),
            hash_builder: ahash::RandomState::new(),
            state: Mutex::new(CompactQueryLabelArenaState::default()),
        })
    }

    fn reserve(&self, bytes: u64) -> io::Result<()> {
        let reserved =
            self.current_bytes
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                    current
                        .checked_add(bytes)
                        .filter(|next| *next <= self.max_bytes)
                });
        let previous = match reserved {
            Ok(previous) => previous,
            Err(_) => {
                self.admission_refusals.fetch_add(1, Ordering::Relaxed);
                return Err(io::Error::new(
                    io::ErrorKind::OutOfMemory,
                    format!(
                        "query label arena budget of {} bytes refused {bytes} bytes",
                        self.max_bytes
                    ),
                ));
            }
        };
        self.peak_bytes
            .fetch_max(previous.saturating_add(bytes), Ordering::Relaxed);
        Ok(())
    }

    fn release(&self, bytes: u64) {
        self.current_bytes
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                current.checked_sub(bytes)
            })
            .expect("query label arena charge underflow");
    }

    fn category_counter(&self, category: CompactQueryLabelChargeCategory) -> &AtomicU64 {
        match category {
            CompactQueryLabelChargeCategory::Atoms => &self.atom_bytes,
            CompactQueryLabelChargeCategory::Pairs => &self.pair_bytes,
            CompactQueryLabelChargeCategory::HashDirectory => &self.hash_directory_bytes,
            CompactQueryLabelChargeCategory::Translations => &self.translation_bytes,
        }
    }

    fn reserve_category(
        &self,
        category: CompactQueryLabelChargeCategory,
        bytes: u64,
    ) -> io::Result<()> {
        self.reserve(bytes)?;
        self.category_counter(category)
            .fetch_add(bytes, Ordering::Relaxed);
        Ok(())
    }

    fn release_category(&self, category: CompactQueryLabelChargeCategory, bytes: u64) {
        self.category_counter(category)
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                current.checked_sub(bytes)
            })
            .expect("query label category charge underflow");
        self.release(bytes);
    }

    pub(super) fn intern_pairs(
        self: &Arc<Self>,
        labels: Vec<(String, String)>,
    ) -> io::Result<QueryLabels> {
        let pair_count = u64::try_from(labels.len()).map_err(|_| {
            io::Error::new(
                io::ErrorKind::OutOfMemory,
                "compact query label-pair count exceeds u64",
            )
        })?;
        let charged_label_block_bytes = compact_query_label_block_bytes(pair_count)?;
        // The pairs category covers both the Box<[CompactQueryLabelPair]>
        // payload and the one Arc<CompactQueryLabels> allocation that owns it.
        // QueryLabels clones share that block and add no arena charge.
        self.reserve_category(
            CompactQueryLabelChargeCategory::Pairs,
            charged_label_block_bytes,
        )?;
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        let mut pairs = Vec::new();
        if pairs.try_reserve_exact(labels.len()).is_err() {
            drop(state);
            self.release_category(
                CompactQueryLabelChargeCategory::Pairs,
                charged_label_block_bytes,
            );
            return Err(io::Error::new(
                io::ErrorKind::OutOfMemory,
                "compact query label-pair allocation failed",
            ));
        }
        for (name, value) in labels {
            let name_id = match self.intern_locked(&mut state, &name) {
                Ok(id) => id,
                Err(error) => {
                    drop(state);
                    self.release_category(
                        CompactQueryLabelChargeCategory::Pairs,
                        charged_label_block_bytes,
                    );
                    return Err(error);
                }
            };
            let value_id = match self.intern_locked(&mut state, &value) {
                Ok(id) => id,
                Err(error) => {
                    drop(state);
                    self.release_category(
                        CompactQueryLabelChargeCategory::Pairs,
                        charged_label_block_bytes,
                    );
                    return Err(error);
                }
            };
            pairs.push(CompactQueryLabelPair { name_id, value_id });
        }
        state.label_sets = state.label_sets.saturating_add(1);
        state.label_pairs = state.label_pairs.saturating_add(pair_count);
        drop(state);
        Ok(self.labels_from_pairs_reserved(pairs, charged_label_block_bytes))
    }

    fn intern_borrowed(&self, value: &str) -> io::Result<u32> {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        self.intern_locked(&mut state, value)
    }

    fn projected_metric_atom(&self, metric_name_id: u32, suffix: &'static str) -> io::Result<u32> {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        if let Some(&projected_id) = state.derived_metric_atoms.get(&(metric_name_id, suffix)) {
            return Ok(projected_id);
        }

        let metric_name = self.resolve(metric_name_id);
        let mut projected = String::new();
        projected
            .try_reserve_exact(metric_name.len().saturating_add(suffix.len()))
            .map_err(|_| {
                io::Error::new(
                    io::ErrorKind::OutOfMemory,
                    "projected compact metric-name allocation failed",
                )
            })?;
        projected.push_str(metric_name);
        projected.push_str(suffix);
        let projected_id = self.intern_locked(&mut state, &projected)?;

        self.reserve_category(
            CompactQueryLabelChargeCategory::HashDirectory,
            COMPACT_QUERY_LABEL_DERIVED_ENTRY_BYTES,
        )?;
        if state.derived_metric_atoms.try_reserve(1).is_err() {
            self.release_category(
                CompactQueryLabelChargeCategory::HashDirectory,
                COMPACT_QUERY_LABEL_DERIVED_ENTRY_BYTES,
            );
            return Err(io::Error::new(
                io::ErrorKind::OutOfMemory,
                "projected compact metric-name cache allocation failed",
            ));
        }
        state
            .derived_metric_atoms
            .insert((metric_name_id, suffix), projected_id);
        Ok(projected_id)
    }

    fn project_metric_suffix(
        self: &Arc<Self>,
        labels: &CompactQueryLabels,
        suffix: &'static str,
    ) -> io::Result<QueryLabels> {
        if !Arc::ptr_eq(self, &labels.arena) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "compact projected labels belong to a different query arena",
            ));
        }
        let metric_pair_index = labels
            .pairs
            .iter()
            .position(|pair| self.resolve(pair.name_id) == METRIC_NAME_LABEL)
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "verified compact labels are missing __name__ during typed projection",
                )
            })?;
        let metric_name_id = labels.pairs[metric_pair_index].value_id;
        let projected_metric_id = self.projected_metric_atom(metric_name_id, suffix)?;
        let pair_count = u64::try_from(labels.pairs.len()).map_err(|_| {
            io::Error::new(
                io::ErrorKind::OutOfMemory,
                "projected compact label-pair count exceeds u64",
            )
        })?;
        let label_block_bytes = compact_query_label_block_bytes(pair_count)?;
        self.reserve_category(CompactQueryLabelChargeCategory::Pairs, label_block_bytes)?;
        let mut pairs = Vec::new();
        if pairs.try_reserve_exact(labels.pairs.len()).is_err() {
            self.release_category(CompactQueryLabelChargeCategory::Pairs, label_block_bytes);
            return Err(io::Error::new(
                io::ErrorKind::OutOfMemory,
                "projected compact label-pair allocation failed",
            ));
        }
        pairs.extend_from_slice(&labels.pairs);
        pairs[metric_pair_index].value_id = projected_metric_id;
        {
            let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
            state.label_sets = state.label_sets.saturating_add(1);
            state.label_pairs = state.label_pairs.saturating_add(pair_count);
        }
        Ok(self.labels_from_pairs_reserved(pairs, label_block_bytes))
    }

    fn intern_locked(
        &self,
        state: &mut CompactQueryLabelArenaState,
        value: &str,
    ) -> io::Result<u32> {
        let content_hash = self.hash_builder.hash_one(value.as_bytes());
        self.intern_locked_with_hash(state, value, content_hash)
    }

    pub(super) fn intern_locked_with_hash(
        &self,
        state: &mut CompactQueryLabelArenaState,
        value: &str,
        content_hash: u64,
    ) -> io::Result<u32> {
        state.lookups = state.lookups.saturating_add(1);
        if let Some(ids) = state.hash_buckets.get(&content_hash) {
            for &id in ids {
                if self.resolve(id) == value {
                    state.hits = state.hits.saturating_add(1);
                    return Ok(id);
                }
            }
        }

        let id = state.next_atom_id;
        let next_atom_id = id
            .checked_add(1)
            .filter(|next| *next != u32::MAX)
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::OutOfMemory,
                    "compact query label arena exceeds usable u32 atom IDs",
                )
            })?;
        let atom_index = usize::try_from(id).expect("u32 query label ID fits usize");
        let chunk_index = atom_index / COMPACT_QUERY_LABEL_ATOM_CHUNK_LEN;
        let local_index = atom_index % COMPACT_QUERY_LABEL_ATOM_CHUNK_LEN;
        let chunk_slot = self.atom_chunks.get(chunk_index).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::OutOfMemory,
                "compact query label arena exceeds its configured atom directory",
            )
        })?;
        let new_chunk_bytes = if chunk_slot.get().is_none() {
            u64::try_from(
                COMPACT_QUERY_LABEL_ATOM_CHUNK_LEN
                    .saturating_mul(std::mem::size_of::<OnceLock<Arc<str>>>()),
            )
            .map_err(|_| {
                io::Error::new(
                    io::ErrorKind::OutOfMemory,
                    "compact query label atom-chunk charge exceeds u64",
                )
            })?
        } else {
            0
        };
        let content_bytes = u64::try_from(value.len()).map_err(|_| {
            io::Error::new(
                io::ErrorKind::OutOfMemory,
                "compact query label length exceeds u64",
            )
        })?;
        let arc_allocation_bytes = modeled_arc_str_allocation_bytes(content_bytes)?;
        let atom_charge = new_chunk_bytes
            .checked_add(arc_allocation_bytes)
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::OutOfMemory,
                    "compact query label atom charge overflows",
                )
            })?;
        self.reserve_category(CompactQueryLabelChargeCategory::Atoms, atom_charge)?;
        if let Err(error) = self.reserve_category(
            CompactQueryLabelChargeCategory::HashDirectory,
            COMPACT_QUERY_LABEL_HASH_ENTRY_BYTES,
        ) {
            self.release_category(CompactQueryLabelChargeCategory::Atoms, atom_charge);
            return Err(error);
        }

        let mut new_chunk = None;
        if new_chunk_bytes != 0 {
            let mut slots = Vec::new();
            if slots
                .try_reserve_exact(COMPACT_QUERY_LABEL_ATOM_CHUNK_LEN)
                .is_err()
            {
                self.release_category(
                    CompactQueryLabelChargeCategory::HashDirectory,
                    COMPACT_QUERY_LABEL_HASH_ENTRY_BYTES,
                );
                self.release_category(CompactQueryLabelChargeCategory::Atoms, atom_charge);
                return Err(io::Error::new(
                    io::ErrorKind::OutOfMemory,
                    "compact query label atom-chunk allocation failed",
                ));
            }
            slots.resize_with(COMPACT_QUERY_LABEL_ATOM_CHUNK_LEN, OnceLock::new);
            new_chunk = Some(slots.into_boxed_slice());
        }
        let hash_capacity_failed = if let Some(ids) = state.hash_buckets.get_mut(&content_hash) {
            ids.try_reserve(1).is_err()
        } else {
            state.hash_buckets.try_reserve(1).is_err()
        };
        if hash_capacity_failed {
            self.release_category(
                CompactQueryLabelChargeCategory::HashDirectory,
                COMPACT_QUERY_LABEL_HASH_ENTRY_BYTES,
            );
            self.release_category(CompactQueryLabelChargeCategory::Atoms, atom_charge);
            return Err(io::Error::new(
                io::ErrorKind::OutOfMemory,
                "compact query label hash-directory allocation failed",
            ));
        }
        let atom: Arc<str> = Arc::from(value);
        if let Some(new_chunk) = new_chunk
            && chunk_slot.set(new_chunk).is_err()
        {
            self.release_category(
                CompactQueryLabelChargeCategory::HashDirectory,
                COMPACT_QUERY_LABEL_HASH_ENTRY_BYTES,
            );
            self.release_category(CompactQueryLabelChargeCategory::Atoms, atom_charge);
            return Err(io::Error::other(
                "compact query label atom chunk was initialized concurrently",
            ));
        }
        let slot = &chunk_slot
            .get()
            .expect("compact query label chunk was initialized")[local_index];
        if slot.set(atom).is_err() {
            self.release_category(
                CompactQueryLabelChargeCategory::HashDirectory,
                COMPACT_QUERY_LABEL_HASH_ENTRY_BYTES,
            );
            self.release_category(CompactQueryLabelChargeCategory::Atoms, atom_charge);
            return Err(io::Error::other(
                "compact query label atom slot was initialized concurrently",
            ));
        }
        state.hash_buckets.entry(content_hash).or_default().push(id);
        state.next_atom_id = next_atom_id;
        state.misses = state.misses.saturating_add(1);
        state.unique_content_bytes = state.unique_content_bytes.saturating_add(content_bytes);
        Ok(id)
    }

    pub(super) fn resolve(&self, id: u32) -> &str {
        let atom_index = usize::try_from(id).expect("u32 query label ID fits usize");
        let chunk_index = atom_index / COMPACT_QUERY_LABEL_ATOM_CHUNK_LEN;
        let local_index = atom_index % COMPACT_QUERY_LABEL_ATOM_CHUNK_LEN;
        self.atom_chunks[chunk_index]
            .get()
            .expect("published compact query label chunk exists")[local_index]
            .get()
            .expect("published compact query label atom exists")
            .as_ref()
    }

    fn labels_from_pairs_reserved(
        self: &Arc<Self>,
        pairs: Vec<CompactQueryLabelPair>,
        charged_label_block_bytes: u64,
    ) -> QueryLabels {
        QueryLabels(QueryLabelStorage::Compact(Arc::new(CompactQueryLabels {
            pairs: pairs.into_boxed_slice(),
            arena: Arc::clone(self),
            _charge_guard: CompactQueryLabelChargeGuard::from_reserved(
                Arc::clone(self),
                CompactQueryLabelChargeCategory::Pairs,
                charged_label_block_bytes,
            ),
        })))
    }

    pub(super) fn snapshot(&self) -> CompactQueryLabelArenaSnapshot {
        let state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        CompactQueryLabelArenaSnapshot {
            lookups: state.lookups,
            hits: state.hits,
            misses: state.misses,
            unique_content_bytes: state.unique_content_bytes,
            label_sets: state.label_sets,
            label_pairs: state.label_pairs,
            current_bytes: self.current_bytes.load(Ordering::Relaxed),
            peak_bytes: self.peak_bytes.load(Ordering::Relaxed),
            admission_refusals: self.admission_refusals.load(Ordering::Relaxed),
            compatibility_materializations: self
                .compatibility_materializations
                .load(Ordering::Relaxed),
            unique_strings: state.misses,
            budget_bytes: self.max_bytes,
            atom_bytes: self.atom_bytes.load(Ordering::Relaxed),
            pair_bytes: self.pair_bytes.load(Ordering::Relaxed),
            hash_directory_bytes: self.hash_directory_bytes.load(Ordering::Relaxed),
            translation_bytes: self.translation_bytes.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug)]
struct SegmentAtomTranslations {
    provenance: SegmentGenerationProvenance,
    symbol_count: u32,
    pages: Box<[Option<Box<[u32]>>]>,
    // Keep this last so the page directory and admitted pages are dropped
    // before their modeled charge is released.
    charge_guard: CompactQueryLabelChargeGuard,
}

// Vec growth is implementation-specific. Four element widths per logical
// admission is the deliberately conservative portable model used here; RSS
// remains the authority for allocator capacity/slack.
const COMPACT_QUERY_LABEL_TRANSLATION_LIST_ENTRY_BYTES: u64 =
    (4 * std::mem::size_of::<SegmentAtomTranslations>()) as u64;

impl SegmentAtomTranslations {
    fn new(
        provenance: SegmentGenerationProvenance,
        symbol_count: u32,
        arena: Arc<CompactQueryLabelArena>,
    ) -> io::Result<Self> {
        let symbol_count = usize::try_from(symbol_count).map_err(|_| {
            io::Error::new(
                io::ErrorKind::OutOfMemory,
                "segment symbol count exceeds usize",
            )
        })?;
        let page_count = symbol_count
            .checked_add(COMPACT_QUERY_LABEL_TRANSLATION_PAGE_LEN - 1)
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::OutOfMemory,
                    "segment translation page count overflows",
                )
            })?
            / COMPACT_QUERY_LABEL_TRANSLATION_PAGE_LEN;
        let directory_payload_bytes =
            u64::try_from(page_count.saturating_mul(std::mem::size_of::<Option<Box<[u32]>>>()))
                .map_err(|_| {
                    io::Error::new(
                        io::ErrorKind::OutOfMemory,
                        "segment translation directory charge exceeds u64",
                    )
                })?;
        let directory_bytes = directory_payload_bytes;
        arena.reserve_category(
            CompactQueryLabelChargeCategory::Translations,
            directory_bytes,
        )?;
        let mut pages = Vec::new();
        if pages.try_reserve_exact(page_count).is_err() {
            arena.release_category(
                CompactQueryLabelChargeCategory::Translations,
                directory_bytes,
            );
            return Err(io::Error::new(
                io::ErrorKind::OutOfMemory,
                "segment translation directory allocation failed",
            ));
        }
        pages.resize_with(page_count, || None);
        Ok(Self {
            provenance,
            symbol_count: u32::try_from(symbol_count).expect("source symbol count came from u32"),
            pages: pages.into_boxed_slice(),
            charge_guard: CompactQueryLabelChargeGuard::from_reserved(
                arena,
                CompactQueryLabelChargeCategory::Translations,
                directory_bytes,
            ),
        })
    }

    fn lookup(&self, source_id: u32) -> io::Result<Option<u32>> {
        if source_id >= self.symbol_count {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "source symbol ID {source_id} exceeds segment symbol count {}",
                    self.symbol_count
                ),
            ));
        }
        let source_index = usize::try_from(source_id).expect("u32 source symbol ID fits usize");
        let page_index = source_index / COMPACT_QUERY_LABEL_TRANSLATION_PAGE_LEN;
        let local_index = source_index % COMPACT_QUERY_LABEL_TRANSLATION_PAGE_LEN;
        Ok(self.pages[page_index].as_ref().and_then(|page| {
            let value = page[local_index];
            (value != COMPACT_QUERY_LABEL_UNTRANSLATED).then_some(value)
        }))
    }

    fn publish(&mut self, source_id: u32, query_id: u32) -> io::Result<()> {
        if query_id == COMPACT_QUERY_LABEL_UNTRANSLATED {
            return Err(io::Error::new(
                io::ErrorKind::OutOfMemory,
                "query label atom ID collides with translation sentinel",
            ));
        }
        if source_id >= self.symbol_count {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "source symbol ID {source_id} exceeds segment symbol count {}",
                    self.symbol_count
                ),
            ));
        }
        let source_index = usize::try_from(source_id).expect("u32 source symbol ID fits usize");
        let page_index = source_index / COMPACT_QUERY_LABEL_TRANSLATION_PAGE_LEN;
        let local_index = source_index % COMPACT_QUERY_LABEL_TRANSLATION_PAGE_LEN;
        if self.pages[page_index].is_none() {
            let page_bytes = u64::try_from(
                COMPACT_QUERY_LABEL_TRANSLATION_PAGE_LEN.saturating_mul(std::mem::size_of::<u32>()),
            )
            .map_err(|_| {
                io::Error::new(
                    io::ErrorKind::OutOfMemory,
                    "segment translation page charge exceeds u64",
                )
            })?;
            self.charge_guard
                .arena
                .reserve_category(CompactQueryLabelChargeCategory::Translations, page_bytes)?;
            let mut page = Vec::new();
            if page
                .try_reserve_exact(COMPACT_QUERY_LABEL_TRANSLATION_PAGE_LEN)
                .is_err()
            {
                self.charge_guard
                    .arena
                    .release_category(CompactQueryLabelChargeCategory::Translations, page_bytes);
                return Err(io::Error::new(
                    io::ErrorKind::OutOfMemory,
                    "segment translation page allocation failed",
                ));
            }
            page.resize(
                COMPACT_QUERY_LABEL_TRANSLATION_PAGE_LEN,
                COMPACT_QUERY_LABEL_UNTRANSLATED,
            );
            self.pages[page_index] = Some(page.into_boxed_slice());
            self.charge_guard.add_reserved(page_bytes);
        }
        let slot = &mut self.pages[page_index]
            .as_mut()
            .expect("translation page was initialized")[local_index];
        if *slot != COMPACT_QUERY_LABEL_UNTRANSLATED && *slot != query_id {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "source symbol translation changed within one segment generation",
            ));
        }
        *slot = query_id;
        Ok(())
    }
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

    fn from_shared(pairs: Vec<(Arc<str>, Arc<str>)>) -> Self {
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

pub(super) fn intern_query_label_atom<S>(
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
