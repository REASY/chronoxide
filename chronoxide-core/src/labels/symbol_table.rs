use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

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
    Arena {
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
            Self::Arena {
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
                "kind=arena symbols={} hash_to_id_len={} hash_to_id_cap={} hash_collisions_len={} hash_collisions_cap={} arena_len={} arena_cap={} id_to_loc_len={} id_to_loc_cap={}",
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

#[derive(Default)]
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

pub struct ArenaSymbolTable<T: SymbolLocTrait = PackedSymbolLoc> {
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
    pub fn len(&self) -> usize {
        self.id_to_loc.len()
    }

    pub fn is_empty(&self) -> bool {
        self.id_to_loc.is_empty()
    }

    pub fn lookup(&self, symbol: &str) -> Option<SymbolId> {
        let hash = hash_symbol(symbol);
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
        let hash = hash_symbol(symbol);
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
}
