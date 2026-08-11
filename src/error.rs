//! Errors returned by the Dactyl driver.

use thiserror::Error;

/// Coarse operational categories stable enough for callers to branch on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdapterErrorKind {
    Busy,
    Locked,
    Timeout,
    Constraint,
    Conflict,
    Query,
    InvalidOperation,
    ReadOnly,
    Capability,
    Value,
    Storage,
    Transport,
    Protocol,
    Unavailable,
    Cancellation,
    Unknown,
}

/// The backend-neutral public error surface.
#[derive(Debug, Error)]
pub enum DactylError {
    #[error("configuration error: {0}")]
    Config(String),

    #[error("adapter error ({kind:?}): {message}")]
    Adapter {
        kind: AdapterErrorKind,
        message: String,
    },

    #[error("unsupported datastore operation: {0}")]
    UnsupportedOperation(String),

    #[error("column not found: {0}")]
    ColumnNotFound(String),

    #[error("conversion error: {0}")]
    Conversion(String),
}

impl DactylError {
    #[allow(dead_code)]
    pub(crate) fn adapter(kind: AdapterErrorKind, message: impl Into<String>) -> Self {
        Self::Adapter {
            kind,
            message: message.into(),
        }
    }

    pub fn adapter_kind(&self) -> Option<AdapterErrorKind> {
        match self {
            Self::Adapter { kind, .. } => Some(*kind),
            _ => None,
        }
    }

    pub fn is_retryable(&self) -> bool {
        matches!(
            self.adapter_kind(),
            Some(AdapterErrorKind::Busy | AdapterErrorKind::Locked | AdapterErrorKind::Timeout)
        )
    }
}
