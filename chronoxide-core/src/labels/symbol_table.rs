use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use german_str::{GermanStr, MAX_INLINE_BYTES, MAX_LEN};
use lasso::{Key, Rodeo, Spur};
use smol_str::SmolStr;
use thiserror::Error;

use super::{
    SymbolId, U64HashMap, estimate_arc_bytes, estimate_hashmap_table_bytes,
    estimate_vec_buffer_bytes,
};

#[derive(Clone, Debug, Eq, PartialEq, Error)]
pub enum ArenaSymbolTableError {
    #[error("symbol too long for ArenaSymbolTable: len={len} max={max}")]
    SymbolTooLong { len: usize, max: usize },

    #[error("ArenaSymbolTable arena overflow: offset={offset} + len={len} overflows usize")]
    ArenaOverflow { offset: usize, len: usize },

    #[error("ArenaSymbolTable arena full: end={end} exceeds max={max}")]
    ArenaFull {
        offset: usize,
        len: usize,
        end: usize,
        max: usize,
    },

    #[error("too many symbols for ArenaSymbolTable: count={count} max={max}")]
    TooManySymbols { count: usize, max: usize },
}

#[derive(Clone, Debug, Eq, PartialEq, Error)]
pub enum SymbolTableError {
    #[error(transparent)]
    Arena(#[from] ArenaSymbolTableError),

    #[error(transparent)]
    German(#[from] GermanSymbolTableError),

    #[error(transparent)]
    SmolStr(#[from] SmolStrSymbolTableError),
}

pub trait SymbolTable {
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
    fn lookup(&self, symbol: &str) -> Option<SymbolId>;
    fn intern(&mut self, symbol: &str) -> Result<SymbolId, SymbolTableError>;
    fn resolve(&self, id: SymbolId) -> &str;

    fn estimate_allocated_bytes(&self) -> usize;
    fn estimate_used_bytes(&self) -> usize;

    fn stats(&self) -> SymbolTableStats;
}

#[derive(Clone, Debug)]
pub enum SymbolTableStats {
    Arc {
        symbols: usize,
        symbol_to_id_len: usize,
        symbol_to_id_cap: usize,
        id_to_symbol_len: usize,
        id_to_symbol_cap: usize,
    },
    Lasso {
        symbols: usize,
        strings_len: usize,
        strings_cap: usize,
        arena_alloc_bytes: usize,
    },
    German {
        symbols: usize,
        hash_to_id_len: usize,
        hash_to_id_cap: usize,
        hash_collisions_len: usize,
        hash_collisions_cap: usize,
        id_to_symbol_len: usize,
        id_to_symbol_cap: usize,
        estimated_heap_bytes: usize,
    },
    SmolStr {
        symbols: usize,
        hash_to_id_len: usize,
        hash_to_id_cap: usize,
        hash_collisions_len: usize,
        hash_collisions_cap: usize,
        id_to_symbol_len: usize,
        id_to_symbol_cap: usize,
        estimated_heap_bytes: usize,
    },
    Arena {
        symbol_hash_kind: &'static str,
        symbols: usize,
        hash_to_id_len: usize,
        hash_to_id_cap: usize,
        hash_collisions_len: usize,
        hash_collisions_cap: usize,
        arena_len: usize,
        arena_cap: usize,
        id_to_loc_len: usize,
        id_to_loc_cap: usize,
    },
}

impl std::fmt::Display for SymbolTableStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Arc {
                symbols,
                symbol_to_id_len,
                symbol_to_id_cap,
                id_to_symbol_len,
                id_to_symbol_cap,
            } => write!(
                f,
                "kind=arc symbols={} symbol_to_id_len={} symbol_to_id_cap={} id_to_symbol_len={} id_to_symbol_cap={}",
                symbols, symbol_to_id_len, symbol_to_id_cap, id_to_symbol_len, id_to_symbol_cap,
            ),
            Self::Lasso {
                symbols,
                strings_len,
                strings_cap,
                arena_alloc_bytes,
            } => write!(
                f,
                "kind=lasso symbols={} strings_len={} strings_cap={} arena_alloc_bytes={}",
                symbols, strings_len, strings_cap, arena_alloc_bytes
            ),
            Self::German {
                symbols,
                hash_to_id_len,
                hash_to_id_cap,
                hash_collisions_len,
                hash_collisions_cap,
                id_to_symbol_len,
                id_to_symbol_cap,
                estimated_heap_bytes,
            } => write!(
                f,
                "kind=german symbols={} hash_to_id_len={} hash_to_id_cap={} hash_collisions_len={} hash_collisions_cap={} id_to_symbol_len={} id_to_symbol_cap={} estimated_heap_bytes={}",
                symbols,
                hash_to_id_len,
                hash_to_id_cap,
                hash_collisions_len,
                hash_collisions_cap,
                id_to_symbol_len,
                id_to_symbol_cap,
                estimated_heap_bytes,
            ),
            Self::SmolStr {
                symbols,
                hash_to_id_len,
                hash_to_id_cap,
                hash_collisions_len,
                hash_collisions_cap,
                id_to_symbol_len,
                id_to_symbol_cap,
                estimated_heap_bytes,
            } => write!(
                f,
                "kind=smol_str symbols={} hash_to_id_len={} hash_to_id_cap={} hash_collisions_len={} hash_collisions_cap={} id_to_symbol_len={} id_to_symbol_cap={} estimated_heap_bytes={}",
                symbols,
                hash_to_id_len,
                hash_to_id_cap,
                hash_collisions_len,
                hash_collisions_cap,
                id_to_symbol_len,
                id_to_symbol_cap,
                estimated_heap_bytes,
            ),
            Self::Arena {
                symbol_hash_kind,
                symbols,
                hash_to_id_len,
                hash_to_id_cap,
                hash_collisions_len,
                hash_collisions_cap,
                arena_len,
                arena_cap,
                id_to_loc_len,
                id_to_loc_cap,
            } => write!(
                f,
                "kind=arena symbol_hash_kind={} symbols={} hash_to_id_len={} hash_to_id_cap={} hash_collisions_len={} hash_collisions_cap={} arena_len={} arena_cap={} id_to_loc_len={} id_to_loc_cap={}",
                symbol_hash_kind,
                symbols,
                hash_to_id_len,
                hash_to_id_cap,
                hash_collisions_len,
                hash_collisions_cap,
                arena_len,
                arena_cap,
                id_to_loc_len,
                id_to_loc_cap,
            ),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Error)]
pub enum GermanSymbolTableError {
    #[error("symbol too long for GermanSymbolTable: len={len} max={max}")]
    SymbolTooLong { len: usize, max: usize },

    #[error("too many symbols for GermanSymbolTable: count={count} max={max}")]
    TooManySymbols { count: usize, max: usize },
}

#[derive(Clone, Debug, Eq, PartialEq, Error)]
pub enum SmolStrSymbolTableError {
    #[error("too many symbols for SmolStrSymbolTable: count={count} max={max}")]
    TooManySymbols { count: usize, max: usize },
}

fn estimate_hashed_symbol_table_allocated_bytes<S>(
    hash_to_id: &U64HashMap<SymbolId>,
    hash_collisions: &U64HashMap<Vec<SymbolId>>,
    id_to_symbol: &Vec<S>,
    estimated_collision_bytes: usize,
    estimated_heap_bytes: usize,
) -> usize {
    estimate_hashmap_table_bytes(hash_to_id)
        .saturating_add(estimate_hashmap_table_bytes(hash_collisions))
        .saturating_add(estimated_collision_bytes)
        .saturating_add(estimate_vec_buffer_bytes(id_to_symbol))
        .saturating_add(estimated_heap_bytes)
}

fn estimate_hashed_symbol_table_used_bytes<S>(
    hash_to_id: &U64HashMap<SymbolId>,
    hash_collisions: &U64HashMap<Vec<SymbolId>>,
    id_to_symbol: &[S],
    estimated_heap_bytes: usize,
) -> usize {
    let hash_bytes = hash_to_id
        .len()
        .saturating_mul(std::mem::size_of::<(u64, SymbolId)>())
        .saturating_add(
            hash_collisions
                .len()
                .saturating_mul(std::mem::size_of::<(u64, Vec<SymbolId>)>()),
        );
    let collision_bytes = hash_collisions
        .values()
        .map(|ids| ids.len().saturating_mul(std::mem::size_of::<SymbolId>()))
        .fold(0usize, usize::saturating_add);
    let id_to_symbol_bytes = id_to_symbol.len().saturating_mul(std::mem::size_of::<S>());

    hash_bytes
        .saturating_add(collision_bytes)
        .saturating_add(id_to_symbol_bytes)
        .saturating_add(estimated_heap_bytes)
}

macro_rules! hashed_symbol_table_stats {
    ($variant:ident, $table:expr) => {{
        let table = $table;
        SymbolTableStats::$variant {
            symbols: table.len(),
            hash_to_id_len: table.hash_to_id.len(),
            hash_to_id_cap: table.hash_to_id.capacity(),
            hash_collisions_len: table.hash_collisions.len(),
            hash_collisions_cap: table.hash_collisions.capacity(),
            id_to_symbol_len: table.id_to_symbol.len(),
            id_to_symbol_cap: table.id_to_symbol.capacity(),
            estimated_heap_bytes: table.estimated_heap_bytes,
        }
    }};
}

#[derive(Clone, Default)]
pub struct ArcSymbolTable {
    symbol_to_id: HashMap<Arc<str>, SymbolId>,
    id_to_symbol: Vec<Arc<str>>,
    estimated_alloc_bytes: usize,
}

impl ArcSymbolTable {
    fn estimate_allocated_bytes_inner(&self) -> usize {
        estimate_hashmap_table_bytes(&self.symbol_to_id)
            .saturating_add(estimate_vec_buffer_bytes(&self.id_to_symbol))
            .saturating_add(self.estimated_alloc_bytes)
    }

    fn estimate_used_bytes_inner(&self) -> usize {
        let hash_bytes = self
            .symbol_to_id
            .len()
            .saturating_mul(std::mem::size_of::<(Arc<str>, SymbolId)>());
        let id_to_symbol_bytes = self
            .id_to_symbol
            .len()
            .saturating_mul(std::mem::size_of::<Arc<str>>());

        hash_bytes
            .saturating_add(id_to_symbol_bytes)
            .saturating_add(self.estimated_alloc_bytes)
    }
}

impl SymbolTable for ArcSymbolTable {
    fn len(&self) -> usize {
        self.id_to_symbol.len()
    }

    fn lookup(&self, symbol: &str) -> Option<SymbolId> {
        self.symbol_to_id.get(symbol).copied()
    }

    fn intern(&mut self, symbol: &str) -> Result<SymbolId, SymbolTableError> {
        if let Some(id) = self.symbol_to_id.get(symbol) {
            return Ok(*id);
        }

        self.estimated_alloc_bytes = self
            .estimated_alloc_bytes
            .saturating_add(estimate_arc_bytes(symbol.len()));

        let symbol: Arc<str> = Arc::from(symbol);
        let id = SymbolId(self.id_to_symbol.len() as u32);
        self.id_to_symbol.push(symbol.clone());
        self.symbol_to_id.insert(symbol, id);
        Ok(id)
    }

    fn resolve(&self, id: SymbolId) -> &str {
        &self.id_to_symbol[id.0 as usize]
    }

    fn estimate_allocated_bytes(&self) -> usize {
        self.estimate_allocated_bytes_inner()
    }

    fn estimate_used_bytes(&self) -> usize {
        self.estimate_used_bytes_inner()
    }

    fn stats(&self) -> SymbolTableStats {
        SymbolTableStats::Arc {
            symbols: self.len(),
            symbol_to_id_len: self.symbol_to_id.len(),
            symbol_to_id_cap: self.symbol_to_id.capacity(),
            id_to_symbol_len: self.id_to_symbol.len(),
            id_to_symbol_cap: self.id_to_symbol.capacity(),
        }
    }
}

#[derive(Debug, Default)]
pub struct LassoSymbolTable {
    interner: Rodeo,
    estimated_heap_bytes: usize,
}

impl Clone for LassoSymbolTable {
    fn clone(&self) -> Self {
        let mut interner = Rodeo::default();
        for symbol in self.interner.strings() {
            interner.get_or_intern(symbol);
        }
        Self {
            interner,
            estimated_heap_bytes: self.estimated_heap_bytes,
        }
    }
}

impl LassoSymbolTable {
    fn key_to_symbol_id(key: Spur) -> SymbolId {
        SymbolId(key.into_usize() as u32)
    }

    fn symbol_id_to_key(id: SymbolId) -> Spur {
        Spur::try_from_usize(id.0 as usize).expect("invalid SymbolId for LassoSymbolTable")
    }
}

impl SymbolTable for LassoSymbolTable {
    fn len(&self) -> usize {
        self.interner.len()
    }

    fn lookup(&self, symbol: &str) -> Option<SymbolId> {
        self.interner.get(symbol).map(Self::key_to_symbol_id)
    }

    fn intern(&mut self, symbol: &str) -> Result<SymbolId, SymbolTableError> {
        let before = self.interner.len();
        let key = self.interner.get_or_intern(symbol);
        if self.interner.len() > before {
            self.estimated_heap_bytes = self.estimated_heap_bytes.saturating_add(symbol.len());
        }
        Ok(Self::key_to_symbol_id(key))
    }

    fn resolve(&self, id: SymbolId) -> &str {
        let key = Self::symbol_id_to_key(id);
        self.interner.resolve(&key)
    }

    fn estimate_allocated_bytes(&self) -> usize {
        0
    }

    fn estimate_used_bytes(&self) -> usize {
        0
    }

    fn stats(&self) -> SymbolTableStats {
        SymbolTableStats::Lasso {
            symbols: self.len(),
            strings_len: self.interner.len(),
            strings_cap: self.interner.capacity(),
            arena_alloc_bytes: self.interner.current_memory_usage(),
        }
    }
}

#[derive(Clone, Default)]
pub struct GermanSymbolTable {
    hash_to_id: U64HashMap<SymbolId>,
    hash_collisions: U64HashMap<Vec<SymbolId>>,
    id_to_symbol: Vec<GermanStr>,
    estimated_collision_bytes: usize,
    estimated_heap_bytes: usize,
}

impl GermanSymbolTable {
    pub fn len(&self) -> usize {
        self.id_to_symbol.len()
    }

    pub fn is_empty(&self) -> bool {
        self.id_to_symbol.is_empty()
    }

    pub fn lookup(&self, symbol: &str) -> Option<SymbolId> {
        let hash = hash_symbol(symbol);
        let &first = self.hash_to_id.get(&hash)?;
        if self.id_to_symbol[first.0 as usize] == symbol {
            return Some(first);
        }
        if let Some(collisions) = self.hash_collisions.get(&hash) {
            for &id in collisions {
                if self.id_to_symbol[id.0 as usize] == symbol {
                    return Some(id);
                }
            }
        }
        None
    }

    fn try_intern(&mut self, symbol: &str) -> Result<SymbolId, GermanSymbolTableError> {
        let hash = hash_symbol(symbol);
        if let Some(&first) = self.hash_to_id.get(&hash) {
            if self.id_to_symbol[first.0 as usize] == symbol {
                return Ok(first);
            }

            if let Some(collisions) = self.hash_collisions.get(&hash) {
                for &id in collisions {
                    if self.id_to_symbol[id.0 as usize] == symbol {
                        return Ok(id);
                    }
                }
            }

            let id = self.intern_new(symbol)?;
            let collisions = self.hash_collisions.entry(hash).or_default();
            let before = collisions.capacity();
            collisions.push(id);
            let after = collisions.capacity();
            if after > before {
                self.estimated_collision_bytes = self.estimated_collision_bytes.saturating_add(
                    (after - before).saturating_mul(std::mem::size_of::<SymbolId>()),
                );
            }
            return Ok(id);
        }

        let id = self.intern_new(symbol)?;
        self.hash_to_id.insert(hash, id);
        Ok(id)
    }

    pub fn resolve(&self, id: SymbolId) -> &str {
        self.id_to_symbol[id.0 as usize].as_str()
    }

    fn intern_new(&mut self, symbol: &str) -> Result<SymbolId, GermanSymbolTableError> {
        let id_u32: u32 = self.id_to_symbol.len().try_into().map_err(|_| {
            GermanSymbolTableError::TooManySymbols {
                count: self.id_to_symbol.len(),
                max: u32::MAX as usize,
            }
        })?;
        let id = SymbolId(id_u32);

        if symbol.len() > MAX_LEN {
            return Err(GermanSymbolTableError::SymbolTooLong {
                len: symbol.len(),
                max: MAX_LEN,
            });
        }

        let symbol = GermanStr::new(symbol).map_err(|_| GermanSymbolTableError::SymbolTooLong {
            len: symbol.len(),
            max: MAX_LEN,
        })?;

        if symbol.len() > MAX_INLINE_BYTES {
            self.estimated_heap_bytes = self.estimated_heap_bytes.saturating_add(symbol.len());
        }

        self.id_to_symbol.push(symbol);
        Ok(id)
    }

    fn estimate_allocated_bytes_inner(&self) -> usize {
        estimate_hashed_symbol_table_allocated_bytes(
            &self.hash_to_id,
            &self.hash_collisions,
            &self.id_to_symbol,
            self.estimated_collision_bytes,
            self.estimated_heap_bytes,
        )
    }

    fn estimate_used_bytes_inner(&self) -> usize {
        estimate_hashed_symbol_table_used_bytes(
            &self.hash_to_id,
            &self.hash_collisions,
            &self.id_to_symbol,
            self.estimated_heap_bytes,
        )
    }
}

impl SymbolTable for GermanSymbolTable {
    fn len(&self) -> usize {
        GermanSymbolTable::len(self)
    }

    fn lookup(&self, symbol: &str) -> Option<SymbolId> {
        GermanSymbolTable::lookup(self, symbol)
    }

    fn intern(&mut self, symbol: &str) -> Result<SymbolId, SymbolTableError> {
        self.try_intern(symbol).map_err(SymbolTableError::from)
    }

    fn resolve(&self, id: SymbolId) -> &str {
        GermanSymbolTable::resolve(self, id)
    }

    fn estimate_allocated_bytes(&self) -> usize {
        self.estimate_allocated_bytes_inner()
    }

    fn estimate_used_bytes(&self) -> usize {
        self.estimate_used_bytes_inner()
    }

    fn stats(&self) -> SymbolTableStats {
        hashed_symbol_table_stats!(German, self)
    }
}

#[derive(Clone, Default)]
pub struct SmolStrSymbolTable {
    hash_to_id: U64HashMap<SymbolId>,
    hash_collisions: U64HashMap<Vec<SymbolId>>,
    id_to_symbol: Vec<SmolStr>,
    estimated_collision_bytes: usize,
    estimated_heap_bytes: usize,
}

impl SmolStrSymbolTable {
    pub fn len(&self) -> usize {
        self.id_to_symbol.len()
    }

    pub fn is_empty(&self) -> bool {
        self.id_to_symbol.is_empty()
    }

    pub fn lookup(&self, symbol: &str) -> Option<SymbolId> {
        let hash = hash_symbol(symbol);
        let &first = self.hash_to_id.get(&hash)?;
        if self.id_to_symbol[first.0 as usize].as_str() == symbol {
            return Some(first);
        }
        if let Some(collisions) = self.hash_collisions.get(&hash) {
            for &id in collisions {
                if self.id_to_symbol[id.0 as usize].as_str() == symbol {
                    return Some(id);
                }
            }
        }
        None
    }

    fn try_intern(&mut self, symbol: &str) -> Result<SymbolId, SmolStrSymbolTableError> {
        let hash = hash_symbol(symbol);
        if let Some(&first) = self.hash_to_id.get(&hash) {
            if self.id_to_symbol[first.0 as usize].as_str() == symbol {
                return Ok(first);
            }

            if let Some(collisions) = self.hash_collisions.get(&hash) {
                for &id in collisions {
                    if self.id_to_symbol[id.0 as usize].as_str() == symbol {
                        return Ok(id);
                    }
                }
            }

            let id = self.intern_new(symbol)?;
            let collisions = self.hash_collisions.entry(hash).or_default();
            let before = collisions.capacity();
            collisions.push(id);
            let after = collisions.capacity();
            if after > before {
                self.estimated_collision_bytes = self.estimated_collision_bytes.saturating_add(
                    (after - before).saturating_mul(std::mem::size_of::<SymbolId>()),
                );
            }
            return Ok(id);
        }

        let id = self.intern_new(symbol)?;
        self.hash_to_id.insert(hash, id);
        Ok(id)
    }

    pub fn resolve(&self, id: SymbolId) -> &str {
        self.id_to_symbol[id.0 as usize].as_str()
    }

    fn intern_new(&mut self, symbol: &str) -> Result<SymbolId, SmolStrSymbolTableError> {
        let id_u32: u32 = self.id_to_symbol.len().try_into().map_err(|_| {
            SmolStrSymbolTableError::TooManySymbols {
                count: self.id_to_symbol.len(),
                max: u32::MAX as usize,
            }
        })?;
        let id = SymbolId(id_u32);

        let symbol = SmolStr::new(symbol);

        // Best-effort: only heap-allocated strings contribute extra buffers beyond `Vec<SmolStr>`.
        // TrackingAllocator output is the authoritative measurement; this is only for estimates.
        if symbol.is_heap_allocated() {
            self.estimated_heap_bytes = self.estimated_heap_bytes.saturating_add(symbol.len());
        }

        self.id_to_symbol.push(symbol);
        Ok(id)
    }

    fn estimate_allocated_bytes_inner(&self) -> usize {
        estimate_hashed_symbol_table_allocated_bytes(
            &self.hash_to_id,
            &self.hash_collisions,
            &self.id_to_symbol,
            self.estimated_collision_bytes,
            self.estimated_heap_bytes,
        )
    }

    fn estimate_used_bytes_inner(&self) -> usize {
        estimate_hashed_symbol_table_used_bytes(
            &self.hash_to_id,
            &self.hash_collisions,
            &self.id_to_symbol,
            self.estimated_heap_bytes,
        )
    }
}

impl SymbolTable for SmolStrSymbolTable {
    fn len(&self) -> usize {
        SmolStrSymbolTable::len(self)
    }

    fn lookup(&self, symbol: &str) -> Option<SymbolId> {
        SmolStrSymbolTable::lookup(self, symbol)
    }

    fn intern(&mut self, symbol: &str) -> Result<SymbolId, SymbolTableError> {
        self.try_intern(symbol).map_err(SymbolTableError::from)
    }

    fn resolve(&self, id: SymbolId) -> &str {
        SmolStrSymbolTable::resolve(self, id)
    }

    fn estimate_allocated_bytes(&self) -> usize {
        self.estimate_allocated_bytes_inner()
    }

    fn estimate_used_bytes(&self) -> usize {
        self.estimate_used_bytes_inner()
    }

    fn stats(&self) -> SymbolTableStats {
        hashed_symbol_table_stats!(SmolStr, self)
    }
}

#[derive(Clone)]
enum ArenaSymbolHash {
    AHash(ahash::RandomState),
    SipHash,
}

impl Default for ArenaSymbolHash {
    fn default() -> Self {
        Self::AHash(ahash::RandomState::new())
    }
}

impl ArenaSymbolHash {
    fn kind(&self) -> &'static str {
        match self {
            Self::AHash(_) => "ahash",
            Self::SipHash => "siphash",
        }
    }

    #[inline]
    fn hash(&self, symbol: &str) -> u64 {
        match self {
            Self::AHash(state) => state.hash_one(symbol),
            Self::SipHash => hash_symbol(symbol),
        }
    }
}

#[derive(Clone)]
pub struct ArenaSymbolTable<T: SymbolLocTrait = PackedSymbolLoc> {
    symbol_hash: ArenaSymbolHash,
    hash_to_id: U64HashMap<SymbolId>,
    hash_collisions: U64HashMap<Vec<SymbolId>>,
    arena: Vec<u8>,
    id_to_loc: Vec<T>,
    estimated_collision_bytes: usize,
    max_arena_bytes: usize,
}

impl<T: SymbolLocTrait> Default for ArenaSymbolTable<T> {
    fn default() -> Self {
        Self {
            symbol_hash: ArenaSymbolHash::default(),
            hash_to_id: Default::default(),
            hash_collisions: Default::default(),
            arena: Default::default(),
            id_to_loc: Default::default(),
            estimated_collision_bytes: 0,
            max_arena_bytes: u32::MAX as usize,
        }
    }
}

impl<T: SymbolLocTrait> ArenaSymbolTable<T> {
    /// Constructs a table using the previous standard-library SipHash
    /// fingerprint for controlled runtime comparisons.
    ///
    /// The fingerprint is only a lookup hint. Full string equality and the
    /// complete collision chain remain authoritative for symbol identity.
    pub fn with_siphash_symbol_hash() -> Self {
        Self {
            symbol_hash: ArenaSymbolHash::SipHash,
            ..Self::default()
        }
    }

    /// Returns the runtime fingerprint implementation used by this table.
    pub fn symbol_hash_kind(&self) -> &'static str {
        self.symbol_hash.kind()
    }

    #[cfg(test)]
    fn with_ahash_seeds_for_test(seeds: [u64; 4]) -> Self {
        Self {
            symbol_hash: ArenaSymbolHash::AHash(ahash::RandomState::with_seeds(
                seeds[0], seeds[1], seeds[2], seeds[3],
            )),
            ..Self::default()
        }
    }

    #[cfg(test)]
    fn symbol_hash_for_test(&self, symbol: &str) -> u64 {
        self.symbol_hash.hash(symbol)
    }

    #[cfg(test)]
    fn intern_with_forced_hash_for_test(
        &mut self,
        symbol: &str,
        hash: u64,
    ) -> Result<SymbolId, ArenaSymbolTableError> {
        self.try_intern_with_hash(symbol, hash)
    }

    #[cfg(test)]
    fn lookup_with_forced_hash_for_test(&self, symbol: &str, hash: u64) -> Option<SymbolId> {
        self.lookup_with_hash(symbol, hash)
    }

    pub fn len(&self) -> usize {
        self.id_to_loc.len()
    }

    pub fn is_empty(&self) -> bool {
        self.id_to_loc.is_empty()
    }

    pub fn lookup(&self, symbol: &str) -> Option<SymbolId> {
        self.lookup_with_hash(symbol, self.symbol_hash.hash(symbol))
    }

    fn lookup_with_hash(&self, symbol: &str, hash: u64) -> Option<SymbolId> {
        let &first = self.hash_to_id.get(&hash)?;
        if self.resolve(first) == symbol {
            return Some(first);
        }
        if let Some(collisions) = self.hash_collisions.get(&hash) {
            for &id in collisions {
                if self.resolve(id) == symbol {
                    return Some(id);
                }
            }
        }
        None
    }

    fn try_intern(&mut self, symbol: &str) -> Result<SymbolId, ArenaSymbolTableError> {
        self.try_intern_with_hash(symbol, self.symbol_hash.hash(symbol))
    }

    fn try_intern_with_hash(
        &mut self,
        symbol: &str,
        hash: u64,
    ) -> Result<SymbolId, ArenaSymbolTableError> {
        if let Some(&first) = self.hash_to_id.get(&hash) {
            if self.resolve(first) == symbol {
                return Ok(first);
            }

            if let Some(collisions) = self.hash_collisions.get(&hash) {
                for &id in collisions {
                    if self.resolve(id) == symbol {
                        return Ok(id);
                    }
                }
            }

            let id = self.intern_new(symbol)?;
            let collisions = self.hash_collisions.entry(hash).or_default();
            let before = collisions.capacity();
            collisions.push(id);
            let after = collisions.capacity();
            if after > before {
                self.estimated_collision_bytes = self.estimated_collision_bytes.saturating_add(
                    (after - before).saturating_mul(std::mem::size_of::<SymbolId>()),
                );
            }
            return Ok(id);
        }

        let id = self.intern_new(symbol)?;
        self.hash_to_id.insert(hash, id);
        Ok(id)
    }

    pub fn resolve(&self, id: SymbolId) -> &str {
        let loc = self.id_to_loc[id.0 as usize];
        let offset = loc.offset() as usize;
        let len = loc.len() as usize;
        let bytes = &self.arena[offset..offset + len];
        unsafe { std::str::from_utf8_unchecked(bytes) }
    }

    fn intern_new(&mut self, symbol: &str) -> Result<SymbolId, ArenaSymbolTableError> {
        let id_u32: u32 =
            self.id_to_loc
                .len()
                .try_into()
                .map_err(|_| ArenaSymbolTableError::TooManySymbols {
                    count: self.id_to_loc.len(),
                    max: u32::MAX as usize,
                })?;
        let id = SymbolId(id_u32);

        let bytes = symbol.as_bytes();
        let len_usize = bytes.len();
        let len: u16 = len_usize
            .try_into()
            .map_err(|_| ArenaSymbolTableError::SymbolTooLong {
                len: len_usize,
                max: u16::MAX as usize,
            })?;

        let offset = self.arena.len();
        let end = offset
            .checked_add(len_usize)
            .ok_or(ArenaSymbolTableError::ArenaOverflow {
                offset,
                len: len_usize,
            })?;

        let max = self.max_arena_bytes.min(u32::MAX as usize);
        if end > max {
            return Err(ArenaSymbolTableError::ArenaFull {
                offset,
                len: len_usize,
                end,
                max,
            });
        }

        let offset = offset as u32;
        self.arena.extend_from_slice(bytes);
        let loc = T::new(offset, len);

        self.id_to_loc.push(loc);
        Ok(id)
    }

    fn estimate_allocated_bytes_inner(&self) -> usize {
        let hash_bytes = estimate_hashmap_table_bytes(&self.hash_to_id)
            .saturating_add(estimate_hashmap_table_bytes(&self.hash_collisions));
        let arena_bytes = estimate_vec_buffer_bytes(&self.arena);
        let id_to_loc_bytes = estimate_vec_buffer_bytes(&self.id_to_loc);

        hash_bytes
            .saturating_add(self.estimated_collision_bytes)
            .saturating_add(arena_bytes)
            .saturating_add(id_to_loc_bytes)
    }

    fn estimate_used_bytes_inner(&self) -> usize {
        let hash_bytes = self
            .hash_to_id
            .len()
            .saturating_mul(std::mem::size_of::<(u64, SymbolId)>())
            .saturating_add(
                self.hash_collisions
                    .len()
                    .saturating_mul(std::mem::size_of::<(u64, Vec<SymbolId>)>()),
            );

        let collision_bytes = self
            .hash_collisions
            .values()
            .map(|ids| ids.len().saturating_mul(std::mem::size_of::<SymbolId>()))
            .fold(0usize, usize::saturating_add);

        let arena_bytes = self.arena.len();
        let id_to_loc_bytes = self
            .id_to_loc
            .len()
            .saturating_mul(std::mem::size_of::<T>());

        hash_bytes
            .saturating_add(collision_bytes)
            .saturating_add(arena_bytes)
            .saturating_add(id_to_loc_bytes)
    }
}

impl<T: SymbolLocTrait> SymbolTable for ArenaSymbolTable<T> {
    fn len(&self) -> usize {
        ArenaSymbolTable::<T>::len(self)
    }

    fn lookup(&self, symbol: &str) -> Option<SymbolId> {
        ArenaSymbolTable::<T>::lookup(self, symbol)
    }

    fn intern(&mut self, symbol: &str) -> Result<SymbolId, SymbolTableError> {
        self.try_intern(symbol).map_err(SymbolTableError::from)
    }

    fn resolve(&self, id: SymbolId) -> &str {
        ArenaSymbolTable::<T>::resolve(self, id)
    }

    fn estimate_allocated_bytes(&self) -> usize {
        self.estimate_allocated_bytes_inner()
    }

    fn estimate_used_bytes(&self) -> usize {
        self.estimate_used_bytes_inner()
    }

    fn stats(&self) -> SymbolTableStats {
        SymbolTableStats::Arena {
            symbol_hash_kind: self.symbol_hash_kind(),
            symbols: self.len(),
            hash_to_id_len: self.hash_to_id.len(),
            hash_to_id_cap: self.hash_to_id.capacity(),
            hash_collisions_len: self.hash_collisions.len(),
            hash_collisions_cap: self.hash_collisions.capacity(),
            arena_len: self.arena.len(),
            arena_cap: self.arena.capacity(),
            id_to_loc_len: self.id_to_loc.len(),
            id_to_loc_cap: self.id_to_loc.capacity(),
        }
    }
}

pub type ArenaSymbolTablePacked = ArenaSymbolTable<PackedSymbolLoc>;
pub type ArenaSymbolTableUnpacked = ArenaSymbolTable<UnpackedSymbolLoc>;
pub type DefaultSymbolTable = ArenaSymbolTablePacked;

pub trait SymbolLocTrait: Copy {
    fn new(offset: u32, len: u16) -> Self;
    fn offset(self) -> u32;
    fn len(self) -> u16;
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(C, packed)]
pub struct PackedSymbolLoc {
    offset: u32,
    len: u16,
}

impl SymbolLocTrait for PackedSymbolLoc {
    fn new(offset: u32, len: u16) -> Self {
        Self { offset, len }
    }

    fn offset(self) -> u32 {
        self.offset
    }

    fn len(self) -> u16 {
        self.len
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(C)]
pub struct UnpackedSymbolLoc {
    offset: u32,
    len: u16,
}

impl SymbolLocTrait for UnpackedSymbolLoc {
    fn new(offset: u32, len: u16) -> Self {
        Self { offset, len }
    }

    fn offset(self) -> u32 {
        self.offset
    }

    fn len(self) -> u16 {
        self.len
    }
}

fn hash_symbol(symbol: &str) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    symbol.hash(&mut hasher);
    hasher.finish()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn arena_symbol_table_defaults_to_ahash_and_exposes_siphash_comparator() {
        let ahash = ArenaSymbolTablePacked::default();
        let siphash = ArenaSymbolTablePacked::with_siphash_symbol_hash();

        assert_eq!(ahash.symbol_hash_kind(), "ahash");
        assert_eq!(siphash.symbol_hash_kind(), "siphash");
        assert!(ahash.stats().to_string().contains("symbol_hash_kind=ahash"));
        assert!(
            siphash
                .stats()
                .to_string()
                .contains("symbol_hash_kind=siphash")
        );
    }

    #[test]
    fn arena_symbol_table_hash_modes_preserve_deterministic_symbol_ids() {
        let trace = [
            "",
            "service.name",
            "checkout",
            "pod",
            "checkout-0",
            "service.name",
            "日本語",
            "checkout-0",
            "embedded\0nul",
        ];
        let seeds_a = [1, 2, 3, 4];
        let seeds_b = [5, 6, 7, 8];
        let mut siphash = ArenaSymbolTablePacked::with_siphash_symbol_hash();
        let mut ahash_a = ArenaSymbolTablePacked::with_ahash_seeds_for_test(seeds_a);
        let mut ahash_a_repeat = ArenaSymbolTablePacked::with_ahash_seeds_for_test(seeds_a);
        let mut ahash_b = ArenaSymbolTablePacked::with_ahash_seeds_for_test(seeds_b);

        for symbol in trace {
            let expected = siphash.intern(symbol).unwrap();
            assert_eq!(ahash_a.intern(symbol).unwrap(), expected);
            assert_eq!(ahash_a_repeat.intern(symbol).unwrap(), expected);
            assert_eq!(ahash_b.intern(symbol).unwrap(), expected);

            assert_eq!(siphash.lookup(symbol), Some(expected));
            assert_eq!(ahash_a.lookup(symbol), Some(expected));
            assert_eq!(ahash_a_repeat.lookup(symbol), Some(expected));
            assert_eq!(ahash_b.lookup(symbol), Some(expected));
        }

        assert_eq!(siphash.len(), ahash_a.len());
        assert_eq!(siphash.len(), ahash_a_repeat.len());
        assert_eq!(siphash.len(), ahash_b.len());
        for index in 0..siphash.len() {
            let id = SymbolId(index as u32);
            let expected = siphash.resolve(id);
            assert_eq!(ahash_a.resolve(id), expected);
            assert_eq!(ahash_a_repeat.resolve(id), expected);
            assert_eq!(ahash_b.resolve(id), expected);
        }

        assert!(trace.iter().all(|symbol| {
            ahash_a.symbol_hash_for_test(symbol) == ahash_a_repeat.symbol_hash_for_test(symbol)
        }));
        assert!(trace.iter().any(|symbol| {
            ahash_a.symbol_hash_for_test(symbol) != ahash_b.symbol_hash_for_test(symbol)
        }));
    }

    #[test]
    fn arena_symbol_table_clone_preserves_hash_mode_and_ahash_keys() {
        let mut ahash = ArenaSymbolTablePacked::with_ahash_seeds_for_test([11, 12, 13, 14]);
        ahash.intern("service.name").unwrap();
        ahash.intern("checkout").unwrap();
        let mut ahash_clone = ahash.clone();

        assert_eq!(ahash_clone.symbol_hash_kind(), "ahash");
        for symbol in ["service.name", "checkout", "new-symbol"] {
            assert_eq!(
                ahash.symbol_hash_for_test(symbol),
                ahash_clone.symbol_hash_for_test(symbol)
            );
        }
        assert_eq!(ahash_clone.lookup("service.name"), Some(SymbolId(0)));
        assert_eq!(ahash.intern("new-symbol"), ahash_clone.intern("new-symbol"));

        let mut siphash = ArenaSymbolTablePacked::with_siphash_symbol_hash();
        siphash.intern("service.name").unwrap();
        let siphash_clone = siphash.clone();
        assert_eq!(siphash_clone.symbol_hash_kind(), "siphash");
        assert_eq!(siphash_clone.lookup("service.name"), Some(SymbolId(0)));
    }

    #[test]
    fn arena_symbol_table_forced_collisions_require_full_string_equality() {
        const FORCED_HASH: u64 = 0xfeed_beef;
        let mut table = ArenaSymbolTablePacked::with_ahash_seeds_for_test([21, 22, 23, 24]);

        let alpha = table
            .intern_with_forced_hash_for_test("alpha", FORCED_HASH)
            .unwrap();
        let beta = table
            .intern_with_forced_hash_for_test("beta", FORCED_HASH)
            .unwrap();
        let gamma = table
            .intern_with_forced_hash_for_test("gamma", FORCED_HASH)
            .unwrap();

        assert_eq!(alpha, SymbolId(0));
        assert_eq!(beta, SymbolId(1));
        assert_eq!(gamma, SymbolId(2));
        assert_eq!(
            table
                .intern_with_forced_hash_for_test("beta", FORCED_HASH)
                .unwrap(),
            beta
        );
        assert_eq!(
            table.lookup_with_forced_hash_for_test("alpha", FORCED_HASH),
            Some(alpha)
        );
        assert_eq!(
            table.lookup_with_forced_hash_for_test("beta", FORCED_HASH),
            Some(beta)
        );
        assert_eq!(
            table.lookup_with_forced_hash_for_test("gamma", FORCED_HASH),
            Some(gamma)
        );
        assert_eq!(
            table.lookup_with_forced_hash_for_test("missing", FORCED_HASH),
            None
        );
        assert_eq!(table.hash_to_id.len(), 1);
        assert_eq!(table.hash_collisions[&FORCED_HASH], vec![beta, gamma]);
    }

    #[test]
    fn arena_symbol_table_symbol_too_long_returns_error() {
        let mut table = ArenaSymbolTablePacked::default();
        let long = "a".repeat(u16::MAX as usize + 1);

        let err = table.intern(long.as_str()).unwrap_err();
        assert_eq!(
            err,
            SymbolTableError::Arena(ArenaSymbolTableError::SymbolTooLong {
                len: u16::MAX as usize + 1,
                max: u16::MAX as usize,
            })
        );
    }

    #[test]
    fn arena_symbol_table_full_returns_error() {
        let mut table = ArenaSymbolTablePacked::default();
        table.max_arena_bytes = 8;

        table.intern("12345678").unwrap();
        let err = table.intern("x").unwrap_err();
        assert!(matches!(
            err,
            SymbolTableError::Arena(ArenaSymbolTableError::ArenaFull { .. })
        ));
    }

    #[test]
    fn german_symbol_table_intern_lookup_resolve_roundtrip() {
        let mut table = GermanSymbolTable::default();

        let id1 = table.intern("service.name").unwrap();
        let id2 = table.intern("service.name").unwrap();
        let id3 = table.intern("instance").unwrap();

        assert_eq!(id1, id2);
        assert_ne!(id1, id3);
        assert_eq!(table.lookup("service.name"), Some(id1));
        assert_eq!(table.lookup("instance"), Some(id3));
        assert_eq!(table.lookup("missing"), None);
        assert_eq!(table.resolve(id1), "service.name");
        assert_eq!(table.resolve(id3), "instance");
    }

    #[test]
    fn smol_str_symbol_table_intern_lookup_resolve_roundtrip() {
        let mut table = SmolStrSymbolTable::default();

        let id1 = table.intern("service.name").unwrap();
        let id2 = table.intern("service.name").unwrap();
        let id3 = table.intern("instance").unwrap();

        assert_eq!(id1, id2);
        assert_ne!(id1, id3);
        assert_eq!(table.lookup("service.name"), Some(id1));
        assert_eq!(table.lookup("instance"), Some(id3));
        assert_eq!(table.lookup("missing"), None);
        assert_eq!(table.resolve(id1), "service.name");
        assert_eq!(table.resolve(id3), "instance");
    }
}
