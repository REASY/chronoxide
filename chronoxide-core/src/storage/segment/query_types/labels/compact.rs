use super::model::{QueryLabelStorage, QueryLabels};
use super::*;

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
pub(in crate::storage::segment::query_types) const COMPACT_QUERY_LABEL_HASH_TABLE_FIXED_RESERVE_BYTES: u64 =
    512;
pub(in crate::storage::segment::query_types) const COMPACT_QUERY_LABEL_ATOM_CHUNK_LEN: usize = 4096;
const COMPACT_QUERY_LABEL_TRANSLATION_PAGE_LEN: usize = 4096;
const COMPACT_QUERY_LABEL_UNTRANSLATED: u32 = u32::MAX;

pub const DEFAULT_QUERY_LABEL_ARENA_MAX_BYTES: u64 = 512 * 1024 * 1024;
pub const MAX_QUERY_LABEL_ARENA_BYTES: u64 = 8 * 1024 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::storage::segment::query_types) struct CompactQueryLabelPair {
    pub(super) name_id: u32,
    pub(super) value_id: u32,
}

#[derive(Debug, Clone, Copy)]
pub(super) enum CompactQueryLabelChargeCategory {
    Atoms,
    Pairs,
    HashDirectory,
    Translations,
}

/// Releases an admitted modeled charge only after payload fields declared
/// before this guard have been dropped.
#[derive(Debug)]
pub(super) struct CompactQueryLabelChargeGuard {
    arena: Arc<CompactQueryLabelArena>,
    category: CompactQueryLabelChargeCategory,
    bytes: AtomicU64,
}

impl CompactQueryLabelChargeGuard {
    pub(super) fn from_reserved(
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

    pub(super) fn add_reserved(&self, bytes: u64) {
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
pub(super) struct CompactQueryLabels {
    pub(super) pairs: Box<[CompactQueryLabelPair]>,
    pub(super) arena: Arc<CompactQueryLabelArena>,
    // Keep this last: fields are dropped in declaration order, so the pair
    // allocation is gone before its modeled charge becomes reusable.
    _charge_guard: CompactQueryLabelChargeGuard,
}

const fn align_up_saturating(value: usize, alignment: usize) -> usize {
    value.saturating_add(alignment.saturating_sub(1)) / alignment * alignment
}

pub(in crate::storage::segment::query_types) const fn modeled_arc_allocation_bytes<T>() -> u64 {
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

pub(in crate::storage::segment::query_types) fn modeled_arc_str_allocation_bytes(
    content_bytes: u64,
) -> io::Result<u64> {
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

pub(in crate::storage::segment::query_types) const COMPACT_QUERY_LABEL_OBJECT_BYTES: u64 =
    modeled_arc_allocation_bytes::<CompactQueryLabels>();

pub(super) fn compact_query_label_block_bytes(pair_count: u64) -> io::Result<u64> {
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

pub(in crate::storage::segment::query_types) type CompactQueryLabelAtomChunk =
    Box<[OnceLock<Arc<str>>]>;
type CompactQueryLabelAtomDirectory = Box<[OnceLock<CompactQueryLabelAtomChunk>]>;

#[derive(Debug)]
pub(in crate::storage::segment::query_types) struct CompactQueryLabelArena {
    max_bytes: u64,
    current_bytes: AtomicU64,
    peak_bytes: AtomicU64,
    atom_bytes: AtomicU64,
    pair_bytes: AtomicU64,
    hash_directory_bytes: AtomicU64,
    translation_bytes: AtomicU64,
    admission_refusals: AtomicU64,
    compatibility_materializations: AtomicU64,
    pub(in crate::storage::segment::query_types) atom_chunks: CompactQueryLabelAtomDirectory,
    hash_builder: ahash::RandomState,
    pub(in crate::storage::segment::query_types) state: Mutex<CompactQueryLabelArenaState>,
}

#[derive(Debug, Default)]
pub(in crate::storage::segment::query_types) struct CompactQueryLabelArenaState {
    hash_buckets: HashMap<u64, SmallVec<[u32; 1]>>,
    derived_metric_atoms: HashMap<(u32, &'static str), u32>,
    next_atom_id: u32,
    pub(in crate::storage::segment::query_types) lookups: u64,
    pub(in crate::storage::segment::query_types) hits: u64,
    pub(in crate::storage::segment::query_types) misses: u64,
    pub(super) unique_content_bytes: u64,
    pub(super) label_sets: u64,
    pub(super) label_pairs: u64,
}

#[derive(Debug, Clone, Copy, Default)]
pub(in crate::storage::segment::query_types) struct CompactQueryLabelArenaSnapshot {
    pub(super) lookups: u64,
    pub(super) hits: u64,
    pub(super) misses: u64,
    pub(super) unique_content_bytes: u64,
    pub(super) label_sets: u64,
    pub(super) label_pairs: u64,
    pub(in crate::storage::segment::query_types) current_bytes: u64,
    pub(in crate::storage::segment::query_types) peak_bytes: u64,
    pub(super) admission_refusals: u64,
    pub(super) compatibility_materializations: u64,
    pub(super) unique_strings: u64,
    pub(super) budget_bytes: u64,
    pub(in crate::storage::segment::query_types) atom_bytes: u64,
    pub(in crate::storage::segment::query_types) pair_bytes: u64,
    pub(in crate::storage::segment::query_types) hash_directory_bytes: u64,
    pub(in crate::storage::segment::query_types) translation_bytes: u64,
}

impl CompactQueryLabelArena {
    pub(in crate::storage::segment::query_types) fn new(max_bytes: u64) -> io::Result<Self> {
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

    pub(super) fn reserve_category(
        &self,
        category: CompactQueryLabelChargeCategory,
        bytes: u64,
    ) -> io::Result<()> {
        self.reserve(bytes)?;
        self.category_counter(category)
            .fetch_add(bytes, Ordering::Relaxed);
        Ok(())
    }

    pub(super) fn release_category(&self, category: CompactQueryLabelChargeCategory, bytes: u64) {
        self.category_counter(category)
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                current.checked_sub(bytes)
            })
            .expect("query label category charge underflow");
        self.release(bytes);
    }

    pub(in crate::storage::segment::query_types) fn intern_pairs(
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

    pub(super) fn intern_borrowed(&self, value: &str) -> io::Result<u32> {
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

    pub(super) fn project_metric_suffix(
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

    pub(in crate::storage::segment::query_types) fn intern_locked_with_hash(
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

    pub(in crate::storage::segment::query_types) fn resolve(&self, id: u32) -> &str {
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

    pub(super) fn labels_from_pairs_reserved(
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

    pub(in crate::storage::segment::query_types) fn snapshot(
        &self,
    ) -> CompactQueryLabelArenaSnapshot {
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
pub(super) struct SegmentAtomTranslations {
    pub(super) provenance: SegmentGenerationProvenance,
    pub(super) symbol_count: u32,
    pages: Box<[Option<Box<[u32]>>]>,
    // Keep this last so the page directory and admitted pages are dropped
    // before their modeled charge is released.
    charge_guard: CompactQueryLabelChargeGuard,
}

// Vec growth is implementation-specific. Four element widths per logical
// admission is the deliberately conservative portable model used here; RSS
// remains the authority for allocator capacity/slack.
pub(super) const COMPACT_QUERY_LABEL_TRANSLATION_LIST_ENTRY_BYTES: u64 =
    (4 * std::mem::size_of::<SegmentAtomTranslations>()) as u64;

impl SegmentAtomTranslations {
    pub(super) fn new(
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

    pub(super) fn lookup(&self, source_id: u32) -> io::Result<Option<u32>> {
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

    pub(super) fn publish(&mut self, source_id: u32, query_id: u32) -> io::Result<()> {
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
