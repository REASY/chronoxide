use thiserror::Error;

use super::super::symbol_table::SymbolTableError;
use super::super::{KeyValueRef, SeriesRef};

#[derive(Clone, Debug, Eq, PartialEq, Error)]
pub enum LabelSetStoreError {
    #[error(transparent)]
    SymbolTable(#[from] SymbolTableError),

    #[error("sealed store cannot intern new series")]
    SealedStore,

    #[error("flat interned {layout} locator {field}={value} exceeds representable maximum {max}")]
    LocatorCapacityExceeded {
        layout: &'static str,
        field: &'static str,
        value: usize,
        max: usize,
    },
}

pub trait LabelSetStore {
    fn intern(&mut self, labels: &[KeyValueRef<'_>]) -> Result<SeriesRef, LabelSetStoreError>;
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn visit_labelset(&self, series: SeriesRef, visitor: impl FnMut(&str, &str));

    /// Returns the exact per-key distinct value count if the store can provide it efficiently.
    ///
    /// This is primarily used by report tooling (e.g. per-key cardinality tables). Most store
    /// implementations should keep the default `None`.
    fn key_cardinality(&self, _key: &str) -> Option<usize> {
        None
    }

    /// Best-effort estimate of bytes held by this store (including heap allocations).
    ///
    /// Notes:
    /// - This is an approximation intended for comparing store layouts.
    /// - It does not account for allocator metadata, rounding, or fragmentation.
    fn estimate_size_bytes(&self) -> usize;

    /// Best-effort estimate of bytes used by live elements (more comparable to "payload size" than
    /// to reserved/allocated capacity).
    fn estimate_used_bytes(&self) -> usize;

    /// Alias for `estimate_size_bytes()`.
    fn estimate_size(&self) -> usize {
        self.estimate_size_bytes()
    }
}
