pub mod arena;
pub mod block;
pub mod chunk;
pub mod encoding;
pub mod file_manager;
pub mod head;
pub mod index;
pub mod io;
pub mod manifest;
pub mod metadata_cache;
pub mod metadata_governor;
pub mod metadata_runtime;
pub mod segment;
pub mod series;
pub mod symbols;
pub mod wal;
pub mod wal_replay;

pub(crate) fn floor_div_i64(value: i64, divisor: i64) -> i64 {
    debug_assert!(divisor > 0);
    let quotient = value / divisor;
    let remainder = value % divisor;
    if remainder != 0 && value < 0 {
        quotient - 1
    } else {
        quotient
    }
}
