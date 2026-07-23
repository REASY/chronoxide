mod format;
mod legacy;
mod reader;
#[allow(dead_code)] // Wired into the schema-neutral segment metadata backend next.
mod runtime_reader;
mod writer;

pub use format::{
    DEFAULT_SYMBOL_PAGE_CACHE_MAX_BYTES, SYMBOLS_V2_VERSION_FOR_LAYOUT_AB, SYMBOLS_V3_HEADER_LEN,
    SYMBOLS_V3_MAGIC, SYMBOLS_V3_MAX_PAGE_BYTES, SYMBOLS_V3_MAX_ROOT_BYTES,
    SYMBOLS_V3_PAGE_DESCRIPTOR_LEN, SYMBOLS_V3_PAGE_HEADER_LEN, SYMBOLS_V3_PAGE_MAGIC,
    SYMBOLS_V3_PAGE_TARGET_BYTES, SYMBOLS_V3_PAGE_VERSION, SYMBOLS_V3_VERSION,
};
pub use reader::{
    SegmentSymbolReadAt, SegmentSymbolReadCount, SegmentSymbolReadStats, SegmentSymbolReader,
    SegmentSymbolResourceSnapshot, SymbolRef, read_symbols_bin_v3,
};
pub use writer::write_symbols_bin_v3;

#[allow(unused_imports)]
pub(crate) use runtime_reader::{
    GovernedSymbolCountBinding, GovernedSymbolLogicalStats, GovernedSymbolLookupBatch,
    GovernedSymbolReader, GovernedSymbolReaderError, GovernedSymbolSession,
};

#[cfg(test)]
mod tests;
