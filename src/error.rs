//! Errors returned by the Dactyl driver.

use thiserror::Error;

use crate::budget::{
    unwrap_adapter_prefixes, FailureOrigin, MutationOutcome, LOCAL_PREFLIGHT_PREFIX,
    LOCAL_RESULT_PREFIX, LOCAL_ROLLBACK_PREFIX, REMOTE_UNCERTAIN_PREFIX,
};

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
    Corrupt,
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

    /// Classify whether the error originated from a local [`crate::ResourceBudget`]
    /// or snapshot limit, a remote service policy denial, a malformed/oversized
    /// remote response, or another engine/transport failure.
    pub fn failure_origin(&self) -> FailureOrigin {
        match self {
            Self::Adapter {
                kind,
                code,
                message,
            } => {
                let inner = unwrap_adapter_prefixes(message);
                if inner.starts_with(LOCAL_PREFLIGHT_PREFIX)
                    || inner.starts_with(LOCAL_RESULT_PREFIX)
                    || matches!(
                        code.as_deref(),
                        Some("inspection_budget_exceeded" | "invalid_inspection_budget")
                    )
                {
                    FailureOrigin::LocalBudget
                } else if *kind == AdapterErrorKind::Protocol
                    && !matches!(
                        code.as_deref(),
                        Some("invalid_context" | "malformed_context")
                    )
                {
                    FailureOrigin::MalformedResponse
                } else if matches!(
                    kind,
                    AdapterErrorKind::Authorization
                        | AdapterErrorKind::RateLimited
                        | AdapterErrorKind::Quota
                ) {
                    FailureOrigin::RemotePolicy
                } else {
                    FailureOrigin::Other
                }
            }
            Self::Config(_)
            | Self::UnsupportedOperation(_)
            | Self::ColumnNotFound(_)
            | Self::Conversion(_) => FailureOrigin::Other,
        }
    }

    /// Classify physical mutation state when a write or atomic batch fails.
    ///
    /// A retryable error category is never permission to replay an
    /// [`MutationOutcome::Uncertain`] mutation without caller/service idempotency.
    pub fn mutation_outcome(&self) -> MutationOutcome {
        match self {
            Self::Config(_)
            | Self::UnsupportedOperation(_)
            | Self::ColumnNotFound(_)
            | Self::Conversion(_) => MutationOutcome::NotAttempted,
            Self::Adapter {
                kind,
                code,
                message,
            } => {
                if message.starts_with(REMOTE_UNCERTAIN_PREFIX) {
                    MutationOutcome::Uncertain
                } else if message.starts_with(LOCAL_ROLLBACK_PREFIX) {
                    MutationOutcome::RolledBack
                } else if (code.as_deref() == Some("invalid_token")
                    && message == "invalid managed session bearer")
                    || message.starts_with(LOCAL_PREFLIGHT_PREFIX)
                    || (matches!(
                        kind,
                        AdapterErrorKind::ReadOnly | AdapterErrorKind::Capability
                    ) && !message.contains("rejected request")
                        && !message.contains("HTTP status"))
                    || (matches!(
                        code.as_deref(),
                        Some(
                            "unsupported_value"
                                | "open_connections"
                                | "path_exists"
                                | "backup_destination_is_source"
                                | "inspection_requires_read_only"
                                | "invalid_inspection_budget"
                                | "invalid_context"
                        )
                    ) && !message.contains("rejected request")
                        && !message.contains("HTTP status"))
                    || (matches!(
                        code.as_deref(),
                        Some("authentication_required" | "missing_context")
                    ) && (message.contains("require an authenticated storage context")
                        || message.contains("requires a user bearer token")
                        || message.contains("requires an authenticated storage context")))
                {
                    MutationOutcome::NotAttempted
                } else {
                    MutationOutcome::RolledBack
                }
            }
        }
    }
}
