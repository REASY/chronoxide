use std::io;

use thiserror::Error;

#[derive(Debug, Error)]
#[error(transparent)]
pub struct CaptureError(Box<CaptureErrorKind>);

impl CaptureError {
    pub fn kind(&self) -> &CaptureErrorKind {
        &self.0
    }

    pub fn into_kind(self) -> CaptureErrorKind {
        *self.0
    }
}

#[derive(Debug, Error)]
pub enum CaptureErrorKind {
    #[error("SerdeJsonError: {0}")]
    SerdeJsonError(#[from] serde_json::Error),

    #[error("IoError: {0}")]
    IoError(#[from] io::Error),
}

impl<E> From<E> for CaptureError
where
    CaptureErrorKind: From<E>,
{
    fn from(error: E) -> Self {
        Self(Box::new(CaptureErrorKind::from(error)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::mem::size_of;

    #[test]
    fn capture_error_is_pointer_sized() {
        assert_eq!(size_of::<CaptureError>(), size_of::<usize>());
    }

    #[test]
    fn io_error_preserves_classification_and_display() {
        let error = CaptureError::from(io::Error::new(io::ErrorKind::InvalidData, "broken"));

        assert!(matches!(
            error.kind(),
            CaptureErrorKind::IoError(inner)
                if inner.kind() == io::ErrorKind::InvalidData
        ));
        assert_eq!(error.to_string(), "IoError: broken");
    }

    #[test]
    fn json_error_preserves_classification_and_display() {
        let serde_error = serde_json::from_slice::<serde_json::Value>(b"{").unwrap_err();
        let error = CaptureError::from(serde_error);

        assert!(matches!(error.kind(), CaptureErrorKind::SerdeJsonError(_)));
        assert!(
            error
                .to_string()
                .starts_with("SerdeJsonError: EOF while parsing")
        );
    }

    #[test]
    fn into_kind_returns_the_owned_error_kind() {
        let error = CaptureError::from(io::Error::new(io::ErrorKind::NotFound, "missing"));

        assert!(matches!(
            error.into_kind(),
            CaptureErrorKind::IoError(inner)
                if inner.kind() == io::ErrorKind::NotFound
        ));
    }
}
