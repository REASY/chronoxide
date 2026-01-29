pub mod alloc_tracking;
pub mod error;
pub mod labels;
pub mod otlp;
pub mod otlp_capture;
pub mod otlp_labelset;
pub mod source;
pub mod statistics;
pub mod storage;
pub mod telemetry;
pub mod util;

pub mod prelude {
    use crate::error;
    pub type Result<T> = std::result::Result<T, error::ChronoxideError>;
}
