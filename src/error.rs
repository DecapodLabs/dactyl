//! Errors and machine-readable recovery dispositions returned by the Dactyl driver.

use thiserror::Error;

use crate::budget::{
    unwrap_adapter_prefixes, FailureOrigin, MutationOutcome, LOCAL_PREFLIGHT_PREFIX,
    LOCAL_RESULT_PREFIX, LOCAL_ROLLBACK_PREFIX, REMOTE_UNCERTAIN_PREFIX,
};
use crate::connector::{
    CloudRecoveryFacts, CloudResponseMetadata, EffectStatus, ExecutionPhase, ObservationSource,
    OperationAttempt, RecoveryActionCategory,
};
use crate::contract::OperationKind;

/// Unambiguous next operational step derived from typed storage error facts,
/// physical mutation outcome, and bounded response provenance.
///
/// Dactyl never auto-retries mutations or grants authority; callers use this
/// disposition to decide whether to retry, inspect state, reconcile via stable
/// request identity, or stop.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum OperationalAction {
    /// Safe to retry (a non-mutating read, or a write/schema operation proved
    /// not attempted before any physical/remote execution).
    Retry,
    /// Execution rolled back cleanly or hit a state/version conflict; inspect
    /// current authoritative state before deciding whether to continue.
    InspectAndDecide,
    /// A mutating operation may have persisted or its acknowledgement was lost;
    /// reconcile using its stable request/attempt identity rather than
    /// blindly replaying.
    Reconcile,
    /// Authorization, authentication, contract, quota, or permanent failure;
    /// stop and surface the error without replaying.
    Stop,
}

/// Composite, machine-readable recovery disposition combining error kind,
/// backend code, failure origin, physical mutation state, bounded server
/// provenance, correlation identifiers, and bounded `Retry-After` hint.
///
/// Correlation identifiers and retry hints are redacted in [`std::fmt::Debug`].
#[derive(Clone, PartialEq, Eq)]
pub struct OperationalDisposition {
    action: OperationalAction,
    operation_kind: OperationKind,
    error_kind: Option<AdapterErrorKind>,
    code: Option<String>,
    failure_origin: FailureOrigin,
    mutation_outcome: Option<MutationOutcome>,
    effect_status: Option<EffectStatus>,
    observation_source: Option<ObservationSource>,
    request_id: Option<String>,
    attempt_id: Option<String>,
    request_digest: Option<String>,
    retry_after: Option<String>,
    recovery: Option<CloudRecoveryFacts>,
}

impl std::fmt::Debug for OperationalDisposition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OperationalDisposition")
            .field("action", &self.action)
            .field("operation_kind", &self.operation_kind)
            .field("error_kind", &self.error_kind)
            .field("code", &self.code)
            .field("failure_origin", &self.failure_origin)
            .field("mutation_outcome", &self.mutation_outcome)
            .field("effect_status", &self.effect_status)
            .field("observation_source", &self.observation_source)
            .field("has_request_id", &self.request_id.is_some())
            .field("has_attempt_id", &self.attempt_id.is_some())
            .field("has_request_digest", &self.request_digest.is_some())
            .field("has_retry_after", &self.retry_after.is_some())
            .field("recovery", &self.recovery)
            .finish()
    }
}

impl OperationalDisposition {
    /// Recommended next operational action for the caller.
    pub fn action(&self) -> OperationalAction {
        self.action
    }

    /// Physical kind of the operation that failed.
    pub fn operation_kind(&self) -> OperationKind {
        self.operation_kind
    }

    /// Coarse adapter error category when the error originated in an adapter.
    pub fn error_kind(&self) -> Option<AdapterErrorKind> {
        self.error_kind
    }

    /// Stable backend/provider or local preflight code, when available.
    pub fn code(&self) -> Option<&str> {
        self.code.as_deref()
    }

    /// Whether the failure came from a local budget, remote policy, malformed
    /// response, or another transport/engine fault.
    pub fn failure_origin(&self) -> FailureOrigin {
        self.failure_origin
    }

    /// Physical mutation state (`None` for non-mutating [`OperationKind::Read`]).
    pub fn mutation_outcome(&self) -> Option<MutationOutcome> {
        self.mutation_outcome
    }

    /// Server/preflight effect status from [`CloudResponseMetadata::provenance`],
    /// when captured.
    pub fn effect_status(&self) -> Option<EffectStatus> {
        self.effect_status
    }

    /// Origin of the provenance observation, when captured.
    pub fn observation_source(&self) -> Option<ObservationSource> {
        self.observation_source
    }

    /// Bounded server request correlation identifier, when available.
    pub fn request_id(&self) -> Option<&str> {
        self.request_id.as_deref()
    }

    /// Caller-supplied or server-echoed attempt/idempotency identifier, when available.
    pub fn attempt_id(&self) -> Option<&str> {
        self.attempt_id.as_deref()
    }

    /// Caller-supplied or server-echoed request digest, when available.
    pub fn request_digest(&self) -> Option<&str> {
        self.request_digest.as_deref()
    }

    /// Bounded server `Retry-After` header value as sent by the service, when present.
    pub fn retry_after(&self) -> Option<&str> {
        self.retry_after.as_deref()
    }

    /// Parsed integer delta-seconds from [`Self::retry_after`], or from
    /// [`CloudRecoveryFacts::retry_after_seconds`] when no header value is
    /// present; never manufactured when absent.
    pub fn retry_after_seconds(&self) -> Option<u64> {
        self.retry_after
            .as_deref()
            .and_then(|raw| raw.trim().parse::<u64>().ok())
            .or_else(|| self.recovery.as_ref().and_then(|r| r.retry_after_seconds()))
    }

    /// Validated, same-origin server recovery facts (`#138` / Propodus `048`)
    /// from [`CloudResponseMetadata::recovery`], when captured.
    pub fn recovery_facts(&self) -> Option<&CloudRecoveryFacts> {
        self.recovery.as_ref()
    }
}

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
                if matches!(
                    code.as_deref(),
                    Some(
                        "sql_budget_exceeded"
                            | "context_budget_exceeded"
                            | "parameter_budget_exceeded"
                            | "cell_budget_exceeded"
                            | "column_budget_exceeded"
                            | "row_budget_exceeded"
                            | "result_budget_exceeded"
                            | "wire_budget_exceeded"
                            | "batch_budget_exceeded"
                            | "inspection_budget_exceeded"
                            | "invalid_inspection_budget"
                    )
                ) || inner.starts_with(LOCAL_PREFLIGHT_PREFIX)
                    || inner.starts_with(LOCAL_RESULT_PREFIX)
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
                kind: _,
                code,
                message,
            } => {
                if message.starts_with(REMOTE_UNCERTAIN_PREFIX) {
                    MutationOutcome::Uncertain
                } else if message.starts_with(LOCAL_ROLLBACK_PREFIX) {
                    MutationOutcome::RolledBack
                } else if (code.as_deref() == Some("invalid_token")
                    && message == "invalid managed session bearer")
                    || message.starts_with("local preflight:")
                    || message.starts_with(LOCAL_PREFLIGHT_PREFIX)
                    || matches!(
                        code.as_deref(),
                        Some(
                            "unsupported_value"
                                | "open_connections"
                                | "path_exists"
                                | "backup_destination_is_source"
                                | "inspection_requires_read_only"
                                | "invalid_inspection_budget"
                                | "invalid_context"
                                | "invalid_path"
                                | "cloud_connector_required"
                                | "unsupported_schema_inspection"
                                | "unsupported_snapshot_inspection"
                                | "unsupported_integrity_verification"
                                | "unsupported_backup"
                                | "unsupported_recovery"
                                | "secure_recovery_unsupported"
                                | "unsupported_recovery_journal_mode"
                                | "device_proof_required"
                        )
                    )
                {
                    MutationOutcome::NotAttempted
                } else {
                    MutationOutcome::RolledBack
                }
            }
        }
    }

    /// Compute one machine-readable [`OperationalDisposition`] for a failed
    /// storage operation without parsing display strings.
    pub fn operational_disposition(&self, operation_kind: OperationKind) -> OperationalDisposition {
        self.operational_disposition_with_context(operation_kind, None, None)
    }

    /// Compute one machine-readable [`OperationalDisposition`] combining typed
    /// error facts with optional attempt correlation and bounded
    /// [`CloudResponseMetadata`] provenance and retry hints.
    ///
    /// Missing metadata stays `None` and is never manufactured. Blind replay of
    /// an uncertain mutation is never authorized, even when
    /// [`Self::is_retryable`] is `true`.
    pub fn operational_disposition_with_context(
        &self,
        operation_kind: OperationKind,
        attempt: Option<&OperationAttempt>,
        metadata: Option<&CloudResponseMetadata>,
    ) -> OperationalDisposition {
        let error_kind = self.adapter_kind();
        let failure_origin = self.failure_origin();
        let provenance = metadata.and_then(|m| m.provenance());
        let effect_status = provenance.map(|p| p.status());
        let observation_source = provenance.map(|p| p.observation_source());
        let code = self
            .adapter_code()
            .or_else(|| provenance.and_then(|p| p.reason_code()))
            .map(str::to_owned);
        let request_id = provenance
            .and_then(|p| p.request_id())
            .map(str::to_owned)
            .or_else(|| {
                metadata
                    .and_then(|m| m.cloud_header("decapod-cloud-request-id"))
                    .map(str::to_owned)
            });
        let recovery = metadata.and_then(|m| m.recovery()).cloned();
        let attempt_id = provenance
            .and_then(|p| p.attempt_id())
            .or_else(|| attempt.map(|a| a.attempt_id()))
            .or_else(|| recovery.as_ref().and_then(|r| r.attempt_ref()))
            .map(str::to_owned);
        let request_digest = provenance
            .and_then(|p| p.request_digest())
            .or_else(|| attempt.and_then(|a| a.request_digest()))
            .map(str::to_owned);
        let retry_after = metadata.and_then(|m| m.retry_after()).map(str::to_owned);

        let fallback_outcome = self.mutation_outcome();
        let recovery_outcome_unknown = recovery.as_ref().is_some_and(|r| {
            r.action_category() == RecoveryActionCategory::OutcomeUnknown
                || (r.execution_phase() == ExecutionPhase::AfterSend
                    && r.action_category() == RecoveryActionCategory::RecoverableWithinAuthority)
        });
        let mutation_outcome = match operation_kind {
            OperationKind::Read => None,
            OperationKind::Write | OperationKind::Schema => {
                let resolved =
                    if fallback_outcome == MutationOutcome::Uncertain || recovery_outcome_unknown {
                        // Post-send transport/decode faults and after_send/outcome_unknown
                        // server recovery facts remain fail-closed Uncertain regardless of
                        // partial or unverified headers.
                        MutationOutcome::Uncertain
                    } else {
                        match effect_status {
                            Some(EffectStatus::NotAttempted | EffectStatus::Refused) => {
                                MutationOutcome::NotAttempted
                            }
                            Some(EffectStatus::RolledBack) => MutationOutcome::RolledBack,
                            Some(EffectStatus::Uncertain | EffectStatus::CommittedObserved) => {
                                MutationOutcome::Uncertain
                            }
                            Some(EffectStatus::UnsupportedProvenance) | None => {
                                if recovery.as_ref().is_some_and(|r| {
                                    r.execution_phase() == ExecutionPhase::PreDispatch
                                }) {
                                    MutationOutcome::NotAttempted
                                } else {
                                    fallback_outcome
                                }
                            }
                        }
                    };
                Some(resolved)
            }
        };

        let is_transient = |kind: AdapterErrorKind| {
            matches!(
                kind,
                AdapterErrorKind::Busy
                    | AdapterErrorKind::Locked
                    | AdapterErrorKind::Timeout
                    | AdapterErrorKind::Unavailable
                    | AdapterErrorKind::Transport
                    | AdapterErrorKind::RateLimited
            )
        };
        let is_state_conflict = |kind: AdapterErrorKind| {
            matches!(
                kind,
                AdapterErrorKind::VersionConflict
                    | AdapterErrorKind::Conflict
                    | AdapterErrorKind::TransactionAborted
                    | AdapterErrorKind::IdempotencyInProgress
            )
        };

        let action = match (operation_kind, mutation_outcome, recovery.as_ref()) {
            (OperationKind::Write | OperationKind::Schema, Some(MutationOutcome::Uncertain), _) => {
                OperationalAction::Reconcile
            }
            (OperationKind::Read, _, Some(facts)) => match facts.action_category() {
                RecoveryActionCategory::RecoverableWithinAuthority => OperationalAction::Retry,
                RecoveryActionCategory::RefreshReconcileState => {
                    OperationalAction::InspectAndDecide
                }
                RecoveryActionCategory::RequiresHumanAction
                | RecoveryActionCategory::DeniedPermanentlyForScope => OperationalAction::Stop,
                RecoveryActionCategory::OutcomeUnknown => {
                    if error_kind.is_some_and(is_transient) {
                        OperationalAction::Retry
                    } else {
                        OperationalAction::InspectAndDecide
                    }
                }
            },
            (OperationKind::Write | OperationKind::Schema, outcome, Some(facts)) => {
                match facts.action_category() {
                    RecoveryActionCategory::OutcomeUnknown => OperationalAction::Reconcile,
                    RecoveryActionCategory::RequiresHumanAction
                    | RecoveryActionCategory::DeniedPermanentlyForScope => OperationalAction::Stop,
                    RecoveryActionCategory::RefreshReconcileState => {
                        OperationalAction::InspectAndDecide
                    }
                    RecoveryActionCategory::RecoverableWithinAuthority => match outcome {
                        Some(MutationOutcome::NotAttempted) => {
                            if error_kind.is_some_and(is_state_conflict) {
                                OperationalAction::InspectAndDecide
                            } else {
                                OperationalAction::Retry
                            }
                        }
                        Some(MutationOutcome::RolledBack) => OperationalAction::InspectAndDecide,
                        Some(MutationOutcome::Uncertain) => OperationalAction::Reconcile,
                        None => OperationalAction::Stop,
                    },
                }
            }
            (OperationKind::Read, _, None) => match error_kind {
                Some(kind)
                    if is_transient(kind) || kind == AdapterErrorKind::TransactionAborted =>
                {
                    OperationalAction::Retry
                }
                Some(kind) if is_state_conflict(kind) => OperationalAction::InspectAndDecide,
                _ => OperationalAction::Stop,
            },
            (OperationKind::Write | OperationKind::Schema, outcome, None) => match outcome {
                Some(MutationOutcome::Uncertain) => OperationalAction::Reconcile,
                Some(MutationOutcome::NotAttempted) => match error_kind {
                    Some(kind) if is_transient(kind) => OperationalAction::Retry,
                    Some(kind) if is_state_conflict(kind) => OperationalAction::InspectAndDecide,
                    _ => OperationalAction::Stop,
                },
                Some(MutationOutcome::RolledBack) => match error_kind {
                    Some(kind) if is_transient(kind) || is_state_conflict(kind) => {
                        OperationalAction::InspectAndDecide
                    }
                    _ => OperationalAction::Stop,
                },
                None => OperationalAction::Stop,
            },
        };

        OperationalDisposition {
            action,
            operation_kind,
            error_kind,
            code,
            failure_origin,
            mutation_outcome,
            effect_status,
            observation_source,
            request_id,
            attempt_id,
            request_digest,
            retry_after,
            recovery,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn non_english_error_messages_do_not_alter_typed_disposition_or_origin() {
        let local_budget = DactylError::adapter_with_code(
            AdapterErrorKind::Quota,
            "sql_budget_exceeded",
            "エラー: 上限超過",
        );
        assert_eq!(local_budget.failure_origin(), FailureOrigin::LocalBudget);
        let disp = local_budget.operational_disposition(OperationKind::Write);
        assert_eq!(disp.action(), OperationalAction::Stop);
        assert_eq!(disp.code(), Some("sql_budget_exceeded"));
        assert_eq!(disp.failure_origin(), FailureOrigin::LocalBudget);

        let local_cap = DactylError::adapter_with_code(
            AdapterErrorKind::Capability,
            "cloud_connector_required",
            "操作はサポートされていません",
        );
        assert_eq!(local_cap.mutation_outcome(), MutationOutcome::NotAttempted);
        assert_eq!(
            local_cap
                .operational_disposition(OperationKind::Write)
                .action(),
            OperationalAction::Stop
        );

        let remote_cap = DactylError::adapter_with_code(
            AdapterErrorKind::Capability,
            "unsupported_query",
            "クエリ非対応",
        );
        assert_eq!(remote_cap.mutation_outcome(), MutationOutcome::RolledBack);
        assert_eq!(
            remote_cap
                .operational_disposition(OperationKind::Write)
                .action(),
            OperationalAction::Stop
        );
    }

    #[test]
    fn retryable_uncertain_write_requires_reconcile_and_never_manufactures_metadata() {
        let timeout_uncertain = DactylError::adapter_with_code(
            AdapterErrorKind::Timeout,
            "outcome_unknown",
            "remote post-send failure (commit outcome uncertain): timed out",
        );
        assert!(timeout_uncertain.is_retryable());
        assert_eq!(
            timeout_uncertain.mutation_outcome(),
            MutationOutcome::Uncertain
        );

        let attempt = OperationAttempt::new("att-150-unit")
            .unwrap()
            .with_request_digest("sha256:abc123")
            .unwrap();
        let disp = timeout_uncertain.operational_disposition_with_context(
            OperationKind::Write,
            Some(&attempt),
            None,
        );
        assert_eq!(disp.action(), OperationalAction::Reconcile);
        assert_eq!(disp.mutation_outcome(), Some(MutationOutcome::Uncertain));
        assert_eq!(disp.attempt_id(), Some("att-150-unit"));
        assert_eq!(disp.request_digest(), Some("sha256:abc123"));
        assert_eq!(disp.request_id(), None);
        assert_eq!(disp.retry_after(), None);
        assert_eq!(disp.retry_after_seconds(), None);
        assert_eq!(disp.effect_status(), None);
        assert_eq!(disp.observation_source(), None);
        let dbg = format!("{disp:?}");
        assert!(!dbg.contains("att-150-unit"));
        assert!(!dbg.contains("sha256:abc123"));

        // Same Timeout on a Read is safe to Retry.
        let read_disp = timeout_uncertain.operational_disposition(OperationKind::Read);
        assert_eq!(read_disp.action(), OperationalAction::Retry);
        assert_eq!(read_disp.mutation_outcome(), None);
    }
}
