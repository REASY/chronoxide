use super::super::metadata_facade::SegmentEncodedLabels;
use super::super::{
    Arc, AtomicU64, HashMap, HashSet, METRIC_NAME_LABEL, Mutex, OnceLock, Ordering, XxHash64, io,
};
use super::result::SegmentQueryResult;
use crate::storage::metadata_runtime::SegmentGenerationProvenance;
use smallvec::SmallVec;

mod compact;
mod interner;
mod model;

#[cfg(test)]
pub(super) use compact::{
    COMPACT_QUERY_LABEL_ATOM_CHUNK_LEN, COMPACT_QUERY_LABEL_HASH_TABLE_FIXED_RESERVE_BYTES,
    COMPACT_QUERY_LABEL_OBJECT_BYTES, CompactQueryLabelArena, CompactQueryLabelAtomChunk,
    CompactQueryLabelPair, modeled_arc_allocation_bytes, modeled_arc_str_allocation_bytes,
};
pub use compact::{DEFAULT_QUERY_LABEL_ARENA_MAX_BYTES, MAX_QUERY_LABEL_ARENA_BYTES};
pub(in crate::storage::segment) use interner::QueryLabelInterner;
#[cfg(test)]
pub(super) use interner::intern_query_label_atom;
pub use interner::{QueryLabelStoragePolicy, QueryLabelStorageStats};
pub use model::{QueryLabelPairs, QueryLabels};
pub(crate) use model::{query_labels_series_id, shared_query_labels};
