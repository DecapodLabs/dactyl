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
    VersionConflict,
    TransactionAborted,
    IdempotencyConflict,
    IdempotencyInProgress,
    Query,
    InvalidOperation,
    ReadOnly,
    Capability,
    Value,
    Storage,
    Transport,
    Protocol,
    Authentication,
    Authorization,
    RateLimited,
    Quota,
    NotFound,
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
        code: Option<String>,
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
            code: None,
            message: message.into(),
        }
    }

    #[cfg(feature = "neon")]
    pub(crate) fn adapter_with_code(
        kind: AdapterErrorKind,
        code: impl Into<String>,
        message: impl Into<String>,
    ) -> Self {
        Self::Adapter {
            kind,
            code: Some(code.into()),
            message: message.into(),
        }
    }

    pub fn adapter_kind(&self) -> Option<AdapterErrorKind> {
        match self {
            Self::Adapter { kind, .. } => Some(*kind),
            _ => None,
        }
    }

    /// Stable remote/provider code, when the adapter received one.
    pub fn adapter_code(&self) -> Option<&str> {
        match self {
            Self::Adapter { code, .. } => code.as_deref(),
            _ => None,
        }
    }

    pub fn is_retryable(&self) -> bool {
        matches!(
            self.adapter_kind(),
            Some(
                AdapterErrorKind::Busy
                    | AdapterErrorKind::Locked
                    | AdapterErrorKind::Timeout
                    | AdapterErrorKind::Unavailable,
            )
        )
    }
}
