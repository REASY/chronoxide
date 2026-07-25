mod bit_packed;
mod common;
mod fixed_width;
mod flat;
mod keyset;
mod naive;
mod packing;
mod versioned_flat;

pub use bit_packed::BitPackedKeySetLabelSetStore;
pub use common::{LabelSetStore, LabelSetStoreError};
pub use fixed_width::FixedWidthPackedKeySetLabelSetStore;
pub use flat::{
    FlatInternedLabelSetRow, FlatInternedLabelSetStore, FlatInternedLabelSetStoreBufferStats,
};
pub use keyset::{
    KeySetDictEncodedLabelSetStore, KeySetLabelSetStoreBufferStats, KeySetTable, ValueCodeDict,
};
pub use naive::{NaiveLabelSetStore, NaiveLabelSetStoreBufferStats};
pub use versioned_flat::{
    VersionedFlatInternedLabelSetRow, VersionedFlatInternedLabelSetSnapshot,
    VersionedFlatInternedLabelSetStore, VersionedFlatLabelStoreError,
    VersionedFlatLabelStoreMemoryStats, VersionedSymbolTable, VersionedSymbolTableSnapshot,
};

pub(crate) use flat::PreparedInternedKeyValue;

const NAIVE_BUFFER_STATS_TYPE_NAME: &str =
    concat!(module_path!(), "::NaiveLabelSetStoreBufferStats");
const FLAT_BUFFER_STATS_TYPE_NAME: &str =
    concat!(module_path!(), "::FlatInternedLabelSetStoreBufferStats");
const KEYSET_BUFFER_STATS_TYPE_NAME: &str =
    concat!(module_path!(), "::KeySetLabelSetStoreBufferStats");
const PACKED_BUFFER_STATS_TYPE_NAME: &str =
    concat!(module_path!(), "::PackedKeySetLabelSetStoreBufferStats");

#[cfg(test)]
mod tests;
