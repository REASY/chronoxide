use std::collections::HashMap;
use std::hash::{BuildHasherDefault, Hasher};

mod interners;
mod normalizer;
mod symbol_table;

pub use interners::{
    BitPackedKeySetLabelSetStore, FixedWidthPackedKeySetLabelSetStore, FlatInternedLabelSetStore,
    FlatInternedLabelSetStoreBufferStats, KeySetDictEncodedLabelSetStore,
    KeySetLabelSetStoreBufferStats, KeySetTable, LabelSetStore, LabelSetStoreError,
    NaiveLabelSetStore, NaiveLabelSetStoreBufferStats, ValueCodeDict,
};

pub use normalizer::{MAX_LABEL_NAME_BYTES, MAX_LABEL_VALUE_BYTES};

pub use symbol_table::{
    ArcSymbolTable, ArenaSymbolTable, ArenaSymbolTableError, ArenaSymbolTablePacked,
    ArenaSymbolTableUnpacked, DefaultSymbolTable, GermanSymbolTable, GermanSymbolTableError,
    LassoSymbolTable, PackedSymbolLoc, SmolStrSymbolTable, SmolStrSymbolTableError, SymbolLocTrait,
    SymbolTable, SymbolTableError, SymbolTableStats, UnpackedSymbolLoc,
};

pub const METRIC_NAME_LABEL: &str = "__name__";

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub struct SeriesRef(u32);

impl SeriesRef {
    pub const fn new(value: u32) -> Self {
        Self(value)
    }

    pub fn get(self) -> u32 {
        self.0
    }
}

impl From<u32> for SeriesRef {
    fn from(value: u32) -> Self {
        Self::new(value)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub struct SymbolId(u32);

impl SymbolId {
    pub fn get(self) -> u32 {
        self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub struct KeySetId(u32);

impl KeySetId {
    pub fn get(self) -> u32 {
        self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub struct ValueCode(u32);

impl ValueCode {
    pub fn get(self) -> u32 {
        self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct KeyValueRef<'a> {
    pub key: &'a str,
    pub value: &'a str,
}

impl<'a> From<(&'a str, &'a str)> for KeyValueRef<'a> {
    fn from(value: (&'a str, &'a str)) -> Self {
        Self {
            key: value.0,
            value: value.1,
        }
    }
}

const HASHMAP_LOAD_FACTOR_NUM: usize = 7;
const HASHMAP_LOAD_FACTOR_DEN: usize = 8;

#[derive(Clone, Default)]
pub struct U64IdentityHasher(u64);

impl Hasher for U64IdentityHasher {
    fn finish(&self) -> u64 {
        self.0
    }

    fn write(&mut self, bytes: &[u8]) {
        let mut hash = 0xcbf29ce484222325u64;
        for &b in bytes {
            hash ^= u64::from(b);
            hash = hash.wrapping_mul(0x100000001b3);
        }
        self.0 = hash;
    }

    fn write_u64(&mut self, i: u64) {
        self.0 = i;
    }
}

type U64BuildHasher = BuildHasherDefault<U64IdentityHasher>;
type U64HashMap<V> = HashMap<u64, V, U64BuildHasher>;

fn estimate_hashmap_table_bytes<K, V, S>(map: &HashMap<K, V, S>) -> usize {
    let element_capacity = map.capacity();
    if element_capacity == 0 {
        return 0;
    }

    let bucket_count =
        (element_capacity * HASHMAP_LOAD_FACTOR_DEN).div_ceil(HASHMAP_LOAD_FACTOR_NUM);

    let elem_bytes = bucket_count.saturating_mul(std::mem::size_of::<(K, V)>());

    let ctrl_bytes = bucket_count;

    elem_bytes.saturating_add(ctrl_bytes)
}

fn estimate_vec_buffer_bytes<T>(vec: &Vec<T>) -> usize {
    vec.capacity().saturating_mul(std::mem::size_of::<T>())
}

fn estimate_arc_bytes(payload_len: usize) -> usize {
    2 * std::mem::size_of::<usize>() + payload_len
}
