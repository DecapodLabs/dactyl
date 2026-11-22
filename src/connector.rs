//! Logical local/cloud connector boundary and portable session/account types.
//!
//! Applications select local SQLite ([`LogicalTarget::local`]) or Decapod Cloud
//! ([`LogicalTarget::cloud`]) per workspace without mutating process-global
//! environment variables. Dactyl resolves local targets to SQLite and Decapod
//! Cloud targets to the versioned public deployment profile, never falling back
//! to SQLite when cloud resolution or authentication fails.

use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::{
    AdapterErrorKind, Backend, Connection, DactylError, DatastoreRoute, OpenOptions, OperationKind,
    ResourceBudget, StorageContext, SupabaseRpcConfig,
};

#[cfg(feature = "supabase")]
use crate::CloudConnector;

/// Current version of the transport decision and effect provenance contract.
pub const PROVENANCE_CONTRACT_VERSION: u16 = 1;

/// Public Decapod Cloud deployment origin.
pub const DEFAULT_CLOUD_ORIGIN: &str = "https://goyxpcmxetztjlrrcwew.supabase.co";

pub(crate) fn validate_provenance_token(
    value: &str,
    message: &'static str,
) -> Result<(), DactylError> {
    if value.is_empty()
        || value.len() > 128
        || !value
            .bytes()
            .all(|b| b.is_ascii_graphic() && !matches!(b, b'"' | b'\\'))
    {
        return Err(DactylError::adapter_with_code(
            AdapterErrorKind::Protocol,
            "invalid_provenance",
            message,
        ));
    }
    Ok(())
}

pub(crate) fn validate_opaque_credential(
    value: &str,
    message: &'static str,
) -> Result<(), DactylError> {
    if value.is_empty() || value.len() > 16384 || !value.bytes().all(|b| b.is_ascii_graphic()) {
        return Err(DactylError::adapter(AdapterErrorKind::Protocol, message));
    }
    Ok(())
}

pub(crate) fn validate_dpop_nonce(value: &str) -> Result<(), DactylError> {
    if value.is_empty()
        || value.len() > 1024
        || !value
            .bytes()
            .all(|b| b.is_ascii_graphic() && !matches!(b, b'"' | b'\\'))
    {
        return Err(DactylError::adapter_with_code(
            AdapterErrorKind::Authentication,
            "invalid_dpop_nonce",
            "local preflight: invalid DPoP nonce",
        ));
    }
    Ok(())
}

#[cfg(feature = "supabase")]
pub(crate) fn validate_bearer_token(value: &str) -> Result<(), DactylError> {
    if value.is_empty() || value.len() > 16384 || !value.bytes().all(|b| b.is_ascii_graphic()) {
        return Err(DactylError::adapter_with_code(
            AdapterErrorKind::Authentication,
            "invalid_token",
            "invalid managed session bearer",
        ));
    }
    Ok(())
}

/// Authorization scheme selected for a managed Decapod Cloud or Supabase RPC request.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum AuthScheme {
    /// Explicit `Authorization: Bearer <token>` compatibility mode.
    #[default]
    Bearer,
    /// Sender-constrained `Authorization: DPoP <token>` + `DPoP: <proof>` mode.
    DPoP,
}

/// Canonical HTTP request metadata passed to a synchronous [`RequestProofSigner`].
///
/// `canonical_url` excludes query strings and fragments per RFC 9449 `htu`
/// binding. `access_token_sha256` is the base64url-unpadded SHA-256 digest of
/// the current access token (`ath`) when an access token is attached.
#[derive(Clone)]
pub struct RequestProofInput<'a> {
    method: &'a str,
    canonical_url: &'a str,
    access_token_sha256: Option<&'a str>,
    nonce: Option<&'a str>,
}

impl std::fmt::Debug for RequestProofInput<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RequestProofInput")
            .field("method", &self.method)
            .field("canonical_url", &"[redacted]")
            .field(
                "has_access_token_sha256",
                &self.access_token_sha256.is_some(),
            )
            .field("has_nonce", &self.nonce.is_some())
            .finish()
    }
}

impl<'a> RequestProofInput<'a> {
    pub fn new(
        method: &'a str,
        canonical_url: &'a str,
        access_token_sha256: Option<&'a str>,
        nonce: Option<&'a str>,
    ) -> Self {
        Self {
            method,
            canonical_url,
            access_token_sha256,
            nonce,
        }
    }

    pub fn method(&self) -> &str {
        self.method
    }

    pub fn canonical_url(&self) -> &str {
        self.canonical_url
    }

    pub fn access_token_sha256(&self) -> Option<&str> {
        self.access_token_sha256
    }

    pub fn nonce(&self) -> Option<&str> {
        self.nonce
    }
}

/// Authorization mode and optional RFC 9449 `DPoP` proof produced by a
/// caller-supplied [`RequestProofSigner`].
#[derive(Clone)]
pub struct RequestProof {
    scheme: AuthScheme,
    #[cfg_attr(not(feature = "supabase"), allow(dead_code))]
    proof: Option<String>,
}

impl std::fmt::Debug for RequestProof {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RequestProof")
            .field("scheme", &self.scheme)
            .field("has_proof", &self.proof.is_some())
            .finish()
    }
}

impl RequestProof {
    /// Select explicit `Authorization: Bearer <token>` compatibility mode.
    pub fn bearer() -> Self {
        Self {
            scheme: AuthScheme::Bearer,
            proof: None,
        }
    }

    /// Attach a fresh RFC 9449 `DPoP` proof (`Authorization: DPoP <token>` + `DPoP: <proof>`).
    pub fn dpop(proof: impl Into<String>) -> Result<Self, DactylError> {
        let proof = proof.into();
        if proof.is_empty() || proof.len() > 16384 || !proof.bytes().all(|b| b.is_ascii_graphic()) {
            return Err(DactylError::adapter_with_code(
                AdapterErrorKind::Authentication,
                "invalid_dpop_proof",
                "local preflight: invalid DPoP proof",
            ));
        }
        Ok(Self {
            scheme: AuthScheme::DPoP,
            proof: Some(proof),
        })
    }

    pub fn scheme(&self) -> AuthScheme {
        self.scheme
    }

    #[cfg(feature = "supabase")]
    pub(crate) fn proof(&self) -> Option<&str> {
        self.proof.as_deref()
    }
}

/// Synchronous caller-supplied proof-signer hook for RFC 9449 `DPoP` proofs
/// or explicit `Bearer` compatibility mode.
///
/// Dactyl never generates keys, accesses OS keystores, runs browser flows, or
/// caches private key material. Callers retain key custody and supply fresh
/// proofs per request.
pub trait RequestProofSigner: Send + Sync {
    fn sign_request(&self, input: &RequestProofInput<'_>) -> Result<RequestProof, DactylError>;

    /// Observe a bounded, validated `DPoP-Nonce` response header returned for
    /// `canonical_url` (on either a success response or a `401` replay/nonce challenge).
    fn observe_nonce(&self, canonical_url: &str, nonce: &str) {
        let _ = (canonical_url, nonce);
    }
}

/// Server-attested or transport-observed execution/effect state for a request.
///
/// Dactyl never upgrades an unadvertised HTTP 200 into [`Self::CommittedObserved`],
/// never fabricates a receipt when provenance is omitted ([`Self::UnsupportedProvenance`]),
/// and never rewrites an earlier [`Self::Uncertain`] client result when a later
/// readback observes server state.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EffectStatus {
    /// Local preflight stopped the request before dispatch, or authoritative
    /// server readback confirmed no attempt was recorded.
    NotAttempted,
    /// Server explicitly refused the request before committing any effect.
    Refused,
    /// Server explicitly established and attested a committed/observed effect
    /// bound to the authenticated origin, auth scheme, and request correlation.
    CommittedObserved,
    /// Authoritative server confirmed the mutation rolled back without effect.
    RolledBack,
    /// Post-send transport loss, timeout, truncated body, redirect, or server
    /// accepted-not-committed state left effect outcome indeterminate.
    Uncertain,
    /// Server returned a response without advertising the versioned provenance
    /// contract; established connector behavior applies with no fabricated receipt.
    UnsupportedProvenance,
}

/// Authoritative server admission decision carried in versioned provenance.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DecisionOutcome {
    Allowed,
    Refused,
}

/// Origin of an [`EffectProvenance`] observation.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ObservationSource {
    /// Observed directly in the HTTP response to the operation request itself.
    DirectResponse,
    /// Observed in a separate, subsequent idempotent server readback call.
    ServerReadback,
    /// Observed locally during preflight checks before any network dispatch.
    LocalPreflight,
    /// Observed after post-send socket loss, timeout, truncation, or redirect.
    TransportFault,
}

/// Caller-supplied stable attempt/idempotency identity and optional request digest
/// and opaque scope binding.
///
/// Preserving the same `attempt_id` and payload across explicit readback or
/// caller-owned idempotent retry avoids duplicate commits on servers enforcing
/// `(attempt_id, request_digest)` idempotency; changing the payload under the
/// same `attempt_id` surfaces a typed conflict when enforced server-side.
#[derive(Clone, PartialEq, Eq)]
pub struct OperationAttempt {
    attempt_id: String,
    request_digest: Option<String>,
    scope_ref: Option<String>,
}

impl std::fmt::Debug for OperationAttempt {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OperationAttempt")
            .field("attempt_id", &"[redacted]")
            .field("has_request_digest", &self.request_digest.is_some())
            .field("has_scope_ref", &self.scope_ref.is_some())
            .finish()
    }
}

impl OperationAttempt {
    pub fn new(attempt_id: impl Into<String>) -> Result<Self, DactylError> {
        let attempt_id = attempt_id.into();
        validate_provenance_token(&attempt_id, "local preflight: invalid attempt identifier")?;
        Ok(Self {
            attempt_id,
            request_digest: None,
            scope_ref: None,
        })
    }

    pub fn with_request_digest(mut self, digest: impl Into<String>) -> Result<Self, DactylError> {
        let digest = digest.into();
        validate_provenance_token(&digest, "local preflight: invalid request digest")?;
        self.request_digest = Some(digest);
        Ok(self)
    }

    pub fn with_scope_ref(mut self, scope_ref: impl Into<String>) -> Result<Self, DactylError> {
        let scope_ref = scope_ref.into();
        validate_provenance_token(&scope_ref, "local preflight: invalid scope reference")?;
        self.scope_ref = Some(scope_ref);
        Ok(self)
    }

    pub fn attempt_id(&self) -> &str {
        &self.attempt_id
    }

    pub fn request_digest(&self) -> Option<&str> {
        self.request_digest.as_deref()
    }

    pub fn scope_ref(&self) -> Option<&str> {
        self.scope_ref.as_deref()
    }
}

/// Bounded, origin- and auth-bound server decision/effect provenance.
///
/// Sensitive correlation references, tokens, origins, and scopes are redacted
/// in [`std::fmt::Debug`].
#[derive(Clone, PartialEq, Eq)]
pub struct EffectProvenance {
    contract_version: Option<u16>,
    status: EffectStatus,
    decision: Option<DecisionOutcome>,
    observation_source: ObservationSource,
    operation_kind: Option<OperationKind>,
    request_id: Option<String>,
    attempt_id: Option<String>,
    request_digest: Option<String>,
    decision_ref: Option<String>,
    reason_code: Option<String>,
    scope_ref: Option<String>,
    auth_scheme: Option<AuthScheme>,
    origin_bound: bool,
    tls_authenticated: bool,
}

impl std::fmt::Debug for EffectProvenance {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EffectProvenance")
            .field("contract_version", &self.contract_version)
            .field("status", &self.status)
            .field("decision", &self.decision)
            .field("observation_source", &self.observation_source)
            .field("operation_kind", &self.operation_kind)
            .field("reason_code", &self.reason_code)
            .field("auth_scheme", &self.auth_scheme)
            .field("origin_bound", &self.origin_bound)
            .field("tls_authenticated", &self.tls_authenticated)
            .field("has_request_id", &self.request_id.is_some())
            .field("has_attempt_id", &self.attempt_id.is_some())
            .field("has_request_digest", &self.request_digest.is_some())
            .field("has_decision_ref", &self.decision_ref.is_some())
            .field("has_scope_ref", &self.scope_ref.is_some())
            .finish()
    }
}

impl EffectProvenance {
    pub fn contract_version(&self) -> Option<u16> {
        self.contract_version
    }

    pub fn status(&self) -> EffectStatus {
        self.status
    }

    pub fn decision(&self) -> Option<DecisionOutcome> {
        self.decision
    }

    pub fn observation_source(&self) -> ObservationSource {
        self.observation_source
    }

    pub fn operation_kind(&self) -> Option<OperationKind> {
        self.operation_kind
    }

    pub fn request_id(&self) -> Option<&str> {
        self.request_id.as_deref()
    }

    pub fn attempt_id(&self) -> Option<&str> {
        self.attempt_id.as_deref()
    }

    pub fn request_digest(&self) -> Option<&str> {
        self.request_digest.as_deref()
    }

    pub fn decision_ref(&self) -> Option<&str> {
        self.decision_ref.as_deref()
    }

    pub fn reason_code(&self) -> Option<&str> {
        self.reason_code.as_deref()
    }

    pub fn scope_ref(&self) -> Option<&str> {
        self.scope_ref.as_deref()
    }

    pub fn auth_scheme(&self) -> Option<AuthScheme> {
        self.auth_scheme
    }

    pub fn origin_bound(&self) -> bool {
        self.origin_bound
    }

    pub fn tls_authenticated(&self) -> bool {
        self.tls_authenticated
    }

    #[cfg(feature = "supabase")]
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn validated(
        status: EffectStatus,
        decision: Option<DecisionOutcome>,
        observation_source: ObservationSource,
        operation_kind: Option<OperationKind>,
        request_id: Option<String>,
        attempt_id: Option<String>,
        request_digest: Option<String>,
        decision_ref: Option<String>,
        reason_code: Option<String>,
        scope_ref: Option<String>,
        auth_scheme: Option<AuthScheme>,
        origin_bound: bool,
        tls_authenticated: bool,
    ) -> Self {
        Self {
            contract_version: Some(PROVENANCE_CONTRACT_VERSION),
            status,
            decision,
            observation_source,
            operation_kind,
            request_id,
            attempt_id,
            request_digest,
            decision_ref,
            reason_code,
            scope_ref,
            auth_scheme,
            origin_bound,
            tls_authenticated,
        }
    }

    pub(crate) fn local_preflight(
        operation_kind: Option<OperationKind>,
        attempt: Option<&OperationAttempt>,
        reason_code: Option<&str>,
        auth_scheme: Option<AuthScheme>,
    ) -> Self {
        Self {
            contract_version: None,
            status: EffectStatus::NotAttempted,
            decision: None,
            observation_source: ObservationSource::LocalPreflight,
            operation_kind,
            request_id: None,
            attempt_id: attempt.map(|a| a.attempt_id.clone()),
            request_digest: attempt.and_then(|a| a.request_digest.clone()),
            decision_ref: None,
            reason_code: reason_code.map(str::to_owned),
            scope_ref: attempt.and_then(|a| a.scope_ref.clone()),
            auth_scheme,
            origin_bound: false,
            tls_authenticated: false,
        }
    }

    #[cfg(feature = "supabase")]
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn transport_fault(
        operation_kind: Option<OperationKind>,
        attempt: Option<&OperationAttempt>,
        resolved_digest: Option<&str>,
        request_id: Option<String>,
        reason_code: Option<&str>,
        auth_scheme: Option<AuthScheme>,
        origin_bound: bool,
        tls_authenticated: bool,
    ) -> Self {
        Self {
            contract_version: None,
            status: EffectStatus::Uncertain,
            decision: None,
            observation_source: ObservationSource::TransportFault,
            operation_kind,
            request_id,
            attempt_id: attempt.map(|a| a.attempt_id.clone()),
            request_digest: resolved_digest
                .map(str::to_owned)
                .or_else(|| attempt.and_then(|a| a.request_digest.clone())),
            decision_ref: None,
            reason_code: reason_code.map(str::to_owned),
            scope_ref: attempt.and_then(|a| a.scope_ref.clone()),
            auth_scheme,
            origin_bound,
            tls_authenticated,
        }
    }

    #[cfg(feature = "supabase")]
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn unsupported(
        observation_source: ObservationSource,
        operation_kind: Option<OperationKind>,
        attempt: Option<&OperationAttempt>,
        resolved_digest: Option<&str>,
        request_id: Option<String>,
        reason_code: Option<String>,
        auth_scheme: Option<AuthScheme>,
        origin_bound: bool,
        tls_authenticated: bool,
    ) -> Self {
        Self {
            contract_version: None,
            status: EffectStatus::UnsupportedProvenance,
            decision: None,
            observation_source,
            operation_kind,
            request_id,
            attempt_id: attempt.map(|a| a.attempt_id.clone()),
            request_digest: resolved_digest
                .map(str::to_owned)
                .or_else(|| attempt.and_then(|a| a.request_digest.clone())),
            decision_ref: None,
            reason_code,
            scope_ref: attempt.and_then(|a| a.scope_ref.clone()),
            auth_scheme,
            origin_bound,
            tls_authenticated,
        }
    }
}

/// Source-attributed server reconciliation result from a separate idempotent readback.
///
/// The earlier client observation (`prior_client_status`, e.g. [`EffectStatus::Uncertain`])
/// is retained unchanged alongside the new [`ObservationSource::ServerReadback`] provenance.
#[derive(Clone, PartialEq)]
pub struct ReconciliationObservation {
    attempt_id: String,
    request_digest: Option<String>,
    provenance: EffectProvenance,
    prior_client_status: Option<EffectStatus>,
    payload: serde_json::Value,
    metadata: CloudResponseMetadata,
}

impl std::fmt::Debug for ReconciliationObservation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReconciliationObservation")
            .field("attempt_id", &"[redacted]")
            .field("has_request_digest", &self.request_digest.is_some())
            .field("prior_client_status", &self.prior_client_status)
            .field("provenance", &self.provenance)
            .field("payload", &"[redacted]")
            .finish()
    }
}

impl ReconciliationObservation {
    #[cfg(feature = "supabase")]
    pub(crate) fn new(
        attempt_id: String,
        request_digest: Option<String>,
        provenance: EffectProvenance,
        prior_client_status: Option<EffectStatus>,
        payload: serde_json::Value,
        metadata: CloudResponseMetadata,
    ) -> Self {
        Self {
            attempt_id,
            request_digest,
            provenance,
            prior_client_status,
            payload,
            metadata,
        }
    }

    pub fn attempt_id(&self) -> &str {
        &self.attempt_id
    }

    pub fn request_digest(&self) -> Option<&str> {
        self.request_digest.as_deref()
    }

    pub fn provenance(&self) -> &EffectProvenance {
        &self.provenance
    }

    pub fn prior_client_status(&self) -> Option<EffectStatus> {
        self.prior_client_status
    }

    pub fn payload(&self) -> &serde_json::Value {
        &self.payload
    }

    pub fn metadata(&self) -> &CloudResponseMetadata {
        &self.metadata
    }
}

/// Bounded, redacted `DPoP-Nonce` and `Decapod-Cloud-*` diagnostic/sunset
/// metadata and validated decision/effect provenance captured from Decapod Cloud
/// HTTP responses.
#[derive(Clone, Default, PartialEq, Eq)]
pub struct CloudResponseMetadata {
    dpop_nonce: Option<String>,
    sunset: Option<String>,
    deprecation: Option<String>,
    retry_after: Option<String>,
    cloud_headers: Vec<(String, String)>,
    provenance: Option<EffectProvenance>,
}

impl std::fmt::Debug for CloudResponseMetadata {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CloudResponseMetadata")
            .field("has_dpop_nonce", &self.dpop_nonce.is_some())
            .field("has_sunset", &self.sunset.is_some())
            .field("has_deprecation", &self.deprecation.is_some())
            .field("has_retry_after", &self.retry_after.is_some())
            .field("cloud_header_count", &self.cloud_headers.len())
            .field("provenance", &self.provenance)
            .finish()
    }
}

impl CloudResponseMetadata {
    pub fn dpop_nonce(&self) -> Option<&str> {
        self.dpop_nonce.as_deref()
    }

    pub fn sunset(&self) -> Option<&str> {
        self.sunset.as_deref()
    }

    pub fn deprecation(&self) -> Option<&str> {
        self.deprecation.as_deref()
    }

    /// Bounded `Retry-After` header as sent by the service, when present.
    pub fn retry_after(&self) -> Option<&str> {
        self.retry_after.as_deref()
    }

    pub fn cloud_headers(&self) -> &[(String, String)] {
        &self.cloud_headers
    }

    pub fn cloud_header(&self, name: &str) -> Option<&str> {
        self.cloud_headers
            .iter()
            .find(|(header, _)| header.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.as_str())
    }

    /// Validated, origin- and auth-bound decision/effect provenance for this response.
    pub fn provenance(&self) -> Option<&EffectProvenance> {
        self.provenance.as_ref()
    }

    #[cfg(feature = "supabase")]
    pub(crate) fn set_dpop_nonce(&mut self, nonce: String) {
        self.dpop_nonce = Some(nonce);
    }

    #[cfg(feature = "supabase")]
    pub(crate) fn set_sunset(&mut self, sunset: String) {
        self.sunset = Some(sunset);
    }

    #[cfg(feature = "supabase")]
    pub(crate) fn set_deprecation(&mut self, deprecation: String) {
        self.deprecation = Some(deprecation);
    }

    #[cfg(feature = "supabase")]
    pub(crate) fn set_retry_after(&mut self, retry_after: String) {
        self.retry_after = Some(retry_after);
    }

    #[cfg(feature = "supabase")]
    pub(crate) fn push_cloud_header(&mut self, name: String, value: String) {
        self.cloud_headers.push((name, value));
    }

    pub(crate) fn set_provenance(&mut self, provenance: EffectProvenance) {
        self.provenance = Some(provenance);
    }
}

/// Account, device-enrollment, and billing operations have fixed backend routes;
/// their versioned payloads, membership, repository authority, seats, pricing,
/// and entitlement decisions remain hosted-service policy.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AccountOperation {
    Register,
    Status,
    Quote,
    Checkout,
    Manage,
    Portal,
    BillingOveragePolicy,
    RepositoryEnroll,
    ResourceLimits,
    OrgAdmins,
    OrgRecovery,
    PolicyTemplates,
    OrgUpgrades,
    DeviceChallenge,
    DeviceEnroll,
    DeviceKeys,
    DeviceSessions,
    /// Issue a project-scoped device session. Keep the returned credentials private.
    DeviceSession,
    /// Rotate a DPoP-bound device session. Keep the returned credentials private.
    DeviceRefresh,
    DeviceStepUp,
    DeviceRevoke,
}

impl AccountOperation {
    /// Fixed onboarding function segment beneath `/functions/v1/decapod-onboarding/`
    /// (with transitional fallback to `/functions/v1/propodus-onboarding/`).
    pub fn action(self) -> &'static str {
        match self {
            Self::Register => "account-register",
            Self::Status => "account-status",
            Self::Quote => "billing-quote",
            Self::Checkout => "billing-checkout",
            Self::Manage => "billing-manage",
            Self::Portal => "billing-portal",
            Self::BillingOveragePolicy => "billing-overage-policy",
            Self::RepositoryEnroll => "repository-enroll",
            Self::ResourceLimits => "resource-limits",
            Self::OrgAdmins => "org-admins",
            Self::OrgRecovery => "org-recovery",
            Self::PolicyTemplates => "policy-templates",
            Self::OrgUpgrades => "org-upgrades",
            Self::DeviceChallenge => "device-challenge",
            Self::DeviceEnroll => "device-enroll",
            Self::DeviceKeys => "device-keys",
            Self::DeviceSessions => "device-sessions",
            Self::DeviceSession => "device-session",
            Self::DeviceRefresh => "device-refresh",
            Self::DeviceStepUp => "device-step-up",
            Self::DeviceRevoke => "device-revoke",
        }
    }

    /// Whether transport loss after dispatch leaves server state uncertain.
    pub fn is_mutating(self) -> bool {
        matches!(
            self,
            Self::Register
                | Self::Checkout
                | Self::Manage
                | Self::Portal
                | Self::BillingOveragePolicy
                | Self::RepositoryEnroll
                | Self::OrgAdmins
                | Self::OrgRecovery
                | Self::PolicyTemplates
                | Self::OrgUpgrades
                | Self::DeviceEnroll
                | Self::DeviceSession
                | Self::DeviceRefresh
                | Self::DeviceStepUp
                | Self::DeviceRevoke
        )
    }

    /// Whether this onboarding or administrative operation requires a human
    /// user bearer session and must reject `dpc_at_` device session tokens.
    pub fn requires_human_bearer(self) -> bool {
        !matches!(self, Self::DeviceRefresh | Self::DeviceRevoke)
    }
}

/// Transport labels for an explicitly supplied SCM linking credential.
/// These labels do not establish identity or backend authorization.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ScmProvider {
    GitHub,
    GitLab,
}

impl ScmProvider {
    #[cfg(feature = "supabase")]
    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::GitHub => "github",
            Self::GitLab => "gitlab",
        }
    }

    pub fn host(self) -> &'static str {
        match self {
            Self::GitHub => "github.com",
            Self::GitLab => "gitlab.com",
        }
    }
}

/// An optional, explicit SCM linking credential for the account channel.
/// The backend independently verifies its provider, subject and freshness.
#[derive(Clone)]
pub struct ScmCredential {
    provider: ScmProvider,
    #[cfg_attr(not(feature = "supabase"), allow(dead_code))]
    token: String,
}

impl std::fmt::Debug for ScmCredential {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ScmCredential")
            .field("provider", &self.provider)
            .field("host", &self.host())
            .field("token", &"[redacted]")
            .finish()
    }
}

impl ScmCredential {
    pub fn github(token: impl Into<String>) -> Result<Self, DactylError> {
        Self::new(ScmProvider::GitHub, token.into())
    }

    pub fn gitlab(token: impl Into<String>) -> Result<Self, DactylError> {
        Self::new(ScmProvider::GitLab, token.into())
    }

    pub fn provider(&self) -> ScmProvider {
        self.provider
    }

    pub fn host(&self) -> &'static str {
        self.provider.host()
    }

    #[cfg(feature = "supabase")]
    pub(crate) fn token(&self) -> &str {
        &self.token
    }

    fn new(provider: ScmProvider, token: String) -> Result<Self, DactylError> {
        validate_opaque_credential(&token, "invalid SCM credential")?;
        Ok(Self { provider, token })
    }
}

/// Durable pending-flow secrets. Serialize only to the caller's private store.
#[derive(Clone, Serialize, Deserialize)]
pub struct AuthenticationSecrets {
    pub(crate) verifier: String,
    pub(crate) poll_secret: String,
}

impl std::fmt::Debug for AuthenticationSecrets {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("AuthenticationSecrets([redacted])")
    }
}

impl AuthenticationSecrets {
    pub fn generate() -> Result<Self, DactylError> {
        #[cfg(feature = "supabase")]
        {
            use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
            fn random() -> Result<String, DactylError> {
                let mut bytes = [0_u8; 32];
                getrandom::getrandom(&mut bytes).map_err(|_| {
                    DactylError::adapter(
                        AdapterErrorKind::Protocol,
                        "secure randomness unavailable",
                    )
                })?;
                Ok(URL_SAFE_NO_PAD.encode(bytes))
            }
            Ok(Self {
                verifier: random()?,
                poll_secret: random()?,
            })
        }
        #[cfg(not(feature = "supabase"))]
        {
            Err(DactylError::Config(
                "cloud support is disabled; enable the `supabase` feature".into(),
            ))
        }
    }

    #[cfg(feature = "supabase")]
    pub(crate) fn challenge(value: &str) -> String {
        use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
        use sha2::{Digest, Sha256};
        URL_SAFE_NO_PAD.encode(Sha256::digest(value.as_bytes()))
    }

    #[cfg(feature = "supabase")]
    pub(crate) fn validate(&self) -> Result<(), DactylError> {
        use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
        for value in [&self.verifier, &self.poll_secret] {
            if value.len() != 43
                || URL_SAFE_NO_PAD
                    .decode(value)
                    .map_or(true, |decoded| decoded.len() != 32)
            {
                return Err(DactylError::adapter(
                    AdapterErrorKind::Protocol,
                    "invalid pending authentication secrets",
                ));
            }
        }
        Ok(())
    }
}

/// Validated browser handoff metadata for a pending cloud authentication flow.
#[derive(Clone, Serialize, Deserialize)]
pub struct AuthenticationStart {
    pub flow_id: String,
    pub browser_url: String,
    pub expires_at: String,
}

impl std::fmt::Debug for AuthenticationStart {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("AuthenticationStart([redacted])")
    }
}

/// A user session issued by managed Auth, never a separate Decapod Cloud session.
#[derive(Clone, Serialize, Deserialize)]
pub struct UserSession {
    pub access_token: String,
    pub refresh_token: String,
    pub expires_at_unix: u64,
    /// Optional credential returned by the sign-in provider. This is not proof
    /// of a linked SCM identity; the account backend verifies that binding.
    /// Refresh may omit it. The caller decides whether to retain prior custody.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_token: Option<String>,
}

impl std::fmt::Debug for UserSession {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("UserSession([redacted])")
    }
}

/// Explicit per-workspace logical connector selection without ambient routing.
#[derive(Clone, PartialEq, Eq)]
pub struct LogicalTarget {
    inner: TargetKind,
}

#[derive(Clone, PartialEq, Eq)]
enum TargetKind {
    Local { path: String },
    Cloud { origin: String },
}

impl std::fmt::Debug for LogicalTarget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LogicalTarget")
            .field("backend", &self.backend())
            .field("target", &"[redacted]")
            .finish()
    }
}

impl LogicalTarget {
    /// Select local SQLite storage at an explicit caller-supplied path.
    pub fn local(path: impl Into<String>) -> Self {
        Self {
            inner: TargetKind::Local { path: path.into() },
        }
    }

    /// Select the built-in cloud deployment origin without reading ambient
    /// process environment variables.
    pub fn cloud() -> Self {
        Self {
            inner: TargetKind::Cloud {
                origin: DEFAULT_CLOUD_ORIGIN.to_owned(),
            },
        }
    }

    /// Select an explicit cloud deployment origin (such as a private deployment
    /// or loopback test fixture) scoped solely to this target instance.
    pub fn cloud_with_origin(origin: impl Into<String>) -> Self {
        Self {
            inner: TargetKind::Cloud {
                origin: origin.into(),
            },
        }
    }

    pub fn backend(&self) -> Backend {
        match &self.inner {
            TargetKind::Local { .. } => Backend::Local,
            TargetKind::Cloud { .. } => Backend::Cloud,
        }
    }
}

/// Resolved per-workspace connector unifying local SQLite and cloud session,
/// readiness, and native RPC datastore access without global state or fallback.
#[derive(Clone)]
pub struct LogicalConnector {
    inner: ConnectorKind,
}

#[derive(Clone)]
enum ConnectorKind {
    Local {
        path: String,
    },
    #[cfg(feature = "supabase")]
    Cloud(Box<CloudConnector>),
}

impl std::fmt::Debug for LogicalConnector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LogicalConnector")
            .field("backend", &self.backend())
            .field("target", &"[redacted]")
            .finish()
    }
}

impl LogicalConnector {
    /// Resolve an explicit [`LogicalTarget`], returning bounded discovery metadata.
    pub fn resolve_with_metadata(
        target: LogicalTarget,
    ) -> Result<(Self, CloudResponseMetadata), DactylError> {
        match target.inner {
            TargetKind::Local { path } => {
                if path.trim().is_empty() {
                    return Err(DactylError::Config(
                        "local storage path must not be empty".into(),
                    ));
                }
                Ok((
                    Self {
                        inner: ConnectorKind::Local { path },
                    },
                    CloudResponseMetadata::default(),
                ))
            }
            TargetKind::Cloud { origin } => {
                #[cfg(feature = "supabase")]
                {
                    let (connector, metadata) = CloudConnector::discover_with_metadata(&origin)?;
                    Ok((
                        Self {
                            inner: ConnectorKind::Cloud(Box::new(connector)),
                        },
                        metadata,
                    ))
                }
                #[cfg(not(feature = "supabase"))]
                {
                    let _ = origin;
                    Err(DactylError::Config(
                        "cloud support is disabled; enable the `supabase` feature".into(),
                    ))
                }
            }
        }
    }

    /// Asynchronously resolve an explicit [`LogicalTarget`], returning bounded discovery metadata.
    pub async fn resolve_with_metadata_async(
        target: LogicalTarget,
    ) -> Result<(Self, CloudResponseMetadata), DactylError> {
        match target.inner {
            TargetKind::Local { path } => {
                if path.trim().is_empty() {
                    return Err(DactylError::Config(
                        "local storage path must not be empty".into(),
                    ));
                }
                Ok((
                    Self {
                        inner: ConnectorKind::Local { path },
                    },
                    CloudResponseMetadata::default(),
                ))
            }
            TargetKind::Cloud { origin } => {
                #[cfg(feature = "supabase")]
                {
                    let (connector, metadata) =
                        CloudConnector::discover_with_metadata_async(&origin).await?;
                    Ok((
                        Self {
                            inner: ConnectorKind::Cloud(Box::new(connector)),
                        },
                        metadata,
                    ))
                }
                #[cfg(not(feature = "supabase"))]
                {
                    let _ = origin;
                    Err(DactylError::Config(
                        "cloud support is disabled; enable the `supabase` feature".into(),
                    ))
                }
            }
        }
    }

    /// Resolve an explicit [`LogicalTarget`].
    ///
    /// Decapod Cloud resolution discovers and validates version 1 of the public
    /// deployment profile without sending user credentials. A failed cloud
    /// discovery or disabled `supabase` feature never falls back to local SQLite.
    pub fn resolve(target: LogicalTarget) -> Result<Self, DactylError> {
        Self::resolve_with_metadata(target).map(|(connector, _)| connector)
    }

    /// Asynchronously resolve an explicit [`LogicalTarget`].
    pub async fn resolve_async(target: LogicalTarget) -> Result<Self, DactylError> {
        Self::resolve_with_metadata_async(target)
            .await
            .map(|(connector, _)| connector)
    }

    /// Configure the HTTP pool and timeout budget for this connector's cloud
    /// auth/account transport (validates `budget` and is otherwise a no-op on
    /// local SQLite connectors).
    pub fn with_http_budget(mut self, budget: ResourceBudget) -> Result<Self, DactylError> {
        budget.validate()?;
        match &mut self.inner {
            ConnectorKind::Local { .. } => {}
            #[cfg(feature = "supabase")]
            ConnectorKind::Cloud(connector) => {
                **connector = (**connector).clone().with_http_budget(budget)?;
            }
        }
        Ok(self)
    }

    /// Attach a caller-supplied [`RequestProofSigner`] for Decapod Cloud and
    /// Supabase RPC requests on this connector (no-op on local SQLite connectors).
    pub fn with_proof_signer(mut self, signer: Arc<dyn RequestProofSigner>) -> Self {
        match &mut self.inner {
            ConnectorKind::Local { .. } => {
                let _ = signer;
            }
            #[cfg(feature = "supabase")]
            ConnectorKind::Cloud(connector) => {
                **connector = (**connector).clone().with_proof_signer(signer);
            }
        }
        self
    }

    /// Supply an initial server `DPoP-Nonce` challenge value for this connector
    /// (no-op on local SQLite connectors).
    pub fn with_dpop_nonce(mut self, nonce: impl Into<String>) -> Self {
        let nonce = nonce.into();
        match &mut self.inner {
            ConnectorKind::Local { .. } => {
                let _ = nonce;
            }
            #[cfg(feature = "supabase")]
            ConnectorKind::Cloud(connector) => {
                **connector = (**connector).clone().with_dpop_nonce(nonce);
            }
        }
        self
    }

    /// Route authenticated Decapod Cloud control and datastore RPCs through the
    /// Edge gateway endpoints (`/functions/v1/decapod-onboarding/cloud-*`) on
    /// this connector (no-op on local SQLite connectors).
    pub fn with_device_gateway(mut self) -> Self {
        match &mut self.inner {
            ConnectorKind::Local { .. } => {}
            #[cfg(feature = "supabase")]
            ConnectorKind::Cloud(connector) => {
                **connector = (**connector).clone().with_device_gateway();
            }
        }
        self
    }

    /// Whether Cloud requests on this connector route through the Edge gateway
    /// (`/functions/v1/decapod-onboarding/cloud-*`). Always `false` for local
    /// SQLite connectors.
    pub fn uses_device_gateway(&self) -> bool {
        match &self.inner {
            ConnectorKind::Local { .. } => false,
            #[cfg(feature = "supabase")]
            ConnectorKind::Cloud(connector) => connector.uses_device_gateway(),
        }
    }

    pub fn local(path: impl Into<String>) -> Result<Self, DactylError> {
        Self::resolve(LogicalTarget::local(path))
    }

    pub fn cloud() -> Result<Self, DactylError> {
        Self::resolve(LogicalTarget::cloud())
    }

    pub async fn cloud_async() -> Result<Self, DactylError> {
        Self::resolve_async(LogicalTarget::cloud()).await
    }

    pub fn cloud_with_origin(origin: impl Into<String>) -> Result<Self, DactylError> {
        Self::resolve(LogicalTarget::cloud_with_origin(origin))
    }

    pub async fn cloud_with_origin_async(origin: impl Into<String>) -> Result<Self, DactylError> {
        Self::resolve_async(LogicalTarget::cloud_with_origin(origin)).await
    }

    /// Construct a cloud connector from an already verified deployment profile
    /// or offline test fixture without performing network discovery.
    pub fn cloud_with_config(config: SupabaseRpcConfig) -> Result<Self, DactylError> {
        #[cfg(feature = "supabase")]
        {
            Ok(Self {
                inner: ConnectorKind::Cloud(Box::new(CloudConnector::with_config(config)?)),
            })
        }
        #[cfg(not(feature = "supabase"))]
        {
            let _ = config;
            Err(DactylError::Config(
                "cloud support is disabled; enable the `supabase` feature".into(),
            ))
        }
    }

    pub fn backend(&self) -> Backend {
        match &self.inner {
            ConnectorKind::Local { .. } => Backend::Local,
            #[cfg(feature = "supabase")]
            ConnectorKind::Cloud(_) => Backend::Cloud,
        }
    }

    #[cfg(feature = "supabase")]
    pub fn cloud_connector(&self) -> Result<&CloudConnector, DactylError> {
        match &self.inner {
            ConnectorKind::Cloud(connector) => Ok(connector),
            ConnectorKind::Local { .. } => Err(local_cloud_capability_error()),
        }
    }

    /// Build a [`DatastoreRoute`] for the resolved connector.
    ///
    /// Local connectors reject cloud bearer credentials. Cloud connectors
    /// validate any supplied bearer token for header safety without network I/O.
    pub fn route(&self, bearer: Option<String>) -> Result<DatastoreRoute, DactylError> {
        match &self.inner {
            ConnectorKind::Local { path } => {
                if bearer.is_some() {
                    return Err(DactylError::Config(
                        "local storage does not accept a cloud credential".into(),
                    ));
                }
                Ok(DatastoreRoute::sqlite(path.clone()))
            }
            #[cfg(feature = "supabase")]
            ConnectorKind::Cloud(connector) => {
                if let Some(ref token) = bearer {
                    validate_bearer_token(token)?;
                }
                Ok(connector.route(bearer))
            }
        }
    }

    /// Open a datastore [`Connection`] for the resolved workspace connector.
    ///
    /// - `Local` opens SQLite with `options` and rejects cloud bearer tokens.
    /// - `Cloud` requires a valid current user `bearer` (failing closed with
    ///   `AdapterErrorKind::Authentication` and code `"authentication_required"`
    ///   when `None` so callers can initiate the missing-session flow on the
    ///   same connector) and requires a caller-owned [`StorageContext`].
    pub fn open(
        &self,
        options: OpenOptions,
        context: Option<StorageContext>,
        bearer: Option<String>,
    ) -> Result<Connection, DactylError> {
        match &self.inner {
            ConnectorKind::Local { path } => {
                if bearer.is_some() {
                    return Err(DactylError::Config(
                        "local storage does not accept a cloud credential".into(),
                    ));
                }
                Connection::open_with_options_and_context(
                    DatastoreRoute::sqlite(path.clone()),
                    options,
                    context,
                )
            }
            #[cfg(feature = "supabase")]
            ConnectorKind::Cloud(connector) => {
                let token = bearer.ok_or_else(|| {
                    DactylError::adapter_with_code(
                        AdapterErrorKind::Authentication,
                        "authentication_required",
                        "cloud storage requires a managed user bearer",
                    )
                })?;
                validate_bearer_token(&token)?;
                Connection::open_with_options_and_context(
                    connector.route(Some(token)),
                    options,
                    context,
                )
            }
        }
    }

    pub fn start_authentication(
        &self,
        repository: &str,
        secrets: &AuthenticationSecrets,
    ) -> Result<AuthenticationStart, DactylError> {
        match &self.inner {
            ConnectorKind::Local { .. } => {
                let _ = (repository, secrets);
                Err(local_cloud_capability_error())
            }
            #[cfg(feature = "supabase")]
            ConnectorKind::Cloud(connector) => connector.start_authentication(repository, secrets),
        }
    }

    pub async fn start_authentication_async(
        &self,
        repository: &str,
        secrets: &AuthenticationSecrets,
    ) -> Result<AuthenticationStart, DactylError> {
        match &self.inner {
            ConnectorKind::Local { .. } => {
                let _ = (repository, secrets);
                Err(local_cloud_capability_error())
            }
            #[cfg(feature = "supabase")]
            ConnectorKind::Cloud(connector) => {
                connector
                    .start_authentication_async(repository, secrets)
                    .await
            }
        }
    }

    pub fn authentication_status(
        &self,
        flow_id: &str,
        secrets: &AuthenticationSecrets,
    ) -> Result<String, DactylError> {
        match &self.inner {
            ConnectorKind::Local { .. } => {
                let _ = (flow_id, secrets);
                Err(local_cloud_capability_error())
            }
            #[cfg(feature = "supabase")]
            ConnectorKind::Cloud(connector) => connector.authentication_status(flow_id, secrets),
        }
    }

    pub async fn authentication_status_async(
        &self,
        flow_id: &str,
        secrets: &AuthenticationSecrets,
    ) -> Result<String, DactylError> {
        match &self.inner {
            ConnectorKind::Local { .. } => {
                let _ = (flow_id, secrets);
                Err(local_cloud_capability_error())
            }
            #[cfg(feature = "supabase")]
            ConnectorKind::Cloud(connector) => {
                connector
                    .authentication_status_async(flow_id, secrets)
                    .await
            }
        }
    }

    /// One-time authorization code exchange. If transport fails after dispatch,
    /// returns error code `"outcome_unknown"` so callers restart rather than replay.
    pub fn exchange_authentication_code(
        &self,
        flow_id: &str,
        secrets: &AuthenticationSecrets,
    ) -> Result<String, DactylError> {
        match &self.inner {
            ConnectorKind::Local { .. } => {
                let _ = (flow_id, secrets);
                Err(local_cloud_capability_error())
            }
            #[cfg(feature = "supabase")]
            ConnectorKind::Cloud(connector) => {
                connector.exchange_authentication_code(flow_id, secrets)
            }
        }
    }

    pub async fn exchange_authentication_code_async(
        &self,
        flow_id: &str,
        secrets: &AuthenticationSecrets,
    ) -> Result<String, DactylError> {
        match &self.inner {
            ConnectorKind::Local { .. } => {
                let _ = (flow_id, secrets);
                Err(local_cloud_capability_error())
            }
            #[cfg(feature = "supabase")]
            ConnectorKind::Cloud(connector) => {
                connector
                    .exchange_authentication_code_async(flow_id, secrets)
                    .await
            }
        }
    }

    /// One-time PKCE verifier exchange. Post-send transport loss returns code
    /// `"outcome_unknown"`.
    pub fn exchange_session(
        &self,
        code: &str,
        secrets: &AuthenticationSecrets,
    ) -> Result<UserSession, DactylError> {
        match &self.inner {
            ConnectorKind::Local { .. } => {
                let _ = (code, secrets);
                Err(local_cloud_capability_error())
            }
            #[cfg(feature = "supabase")]
            ConnectorKind::Cloud(connector) => connector.exchange_session(code, secrets),
        }
    }

    pub async fn exchange_session_async(
        &self,
        code: &str,
        secrets: &AuthenticationSecrets,
    ) -> Result<UserSession, DactylError> {
        match &self.inner {
            ConnectorKind::Local { .. } => {
                let _ = (code, secrets);
                Err(local_cloud_capability_error())
            }
            #[cfg(feature = "supabase")]
            ConnectorKind::Cloud(connector) => {
                connector.exchange_session_async(code, secrets).await
            }
        }
    }

    /// Refresh a managed user session. Post-send transport loss returns code
    /// `"outcome_unknown"` because refresh tokens are single-use.
    pub fn refresh_session(&self, refresh_token: &str) -> Result<UserSession, DactylError> {
        match &self.inner {
            ConnectorKind::Local { .. } => {
                let _ = refresh_token;
                Err(local_cloud_capability_error())
            }
            #[cfg(feature = "supabase")]
            ConnectorKind::Cloud(connector) => connector.refresh_session(refresh_token),
        }
    }

    pub async fn refresh_session_async(
        &self,
        refresh_token: &str,
    ) -> Result<UserSession, DactylError> {
        match &self.inner {
            ConnectorKind::Local { .. } => {
                let _ = refresh_token;
                Err(local_cloud_capability_error())
            }
            #[cfg(feature = "supabase")]
            ConnectorKind::Cloud(connector) => connector.refresh_session_async(refresh_token).await,
        }
    }

    pub fn complete_authentication(
        &self,
        flow_id: &str,
        secrets: &AuthenticationSecrets,
        bearer: &str,
    ) -> Result<(), DactylError> {
        match &self.inner {
            ConnectorKind::Local { .. } => {
                let _ = (flow_id, secrets, bearer);
                Err(local_cloud_capability_error())
            }
            #[cfg(feature = "supabase")]
            ConnectorKind::Cloud(connector) => {
                connector.complete_authentication(flow_id, secrets, bearer)
            }
        }
    }

    pub async fn complete_authentication_async(
        &self,
        flow_id: &str,
        secrets: &AuthenticationSecrets,
        bearer: &str,
    ) -> Result<(), DactylError> {
        match &self.inner {
            ConnectorKind::Local { .. } => {
                let _ = (flow_id, secrets, bearer);
                Err(local_cloud_capability_error())
            }
            #[cfg(feature = "supabase")]
            ConnectorKind::Cloud(connector) => {
                connector
                    .complete_authentication_async(flow_id, secrets, bearer)
                    .await
            }
        }
    }

    /// Revoke the current session scope on the managed Auth service.
    pub fn revoke_session(&self, bearer: &str) -> Result<(), DactylError> {
        match &self.inner {
            ConnectorKind::Local { .. } => {
                let _ = bearer;
                Err(local_cloud_capability_error())
            }
            #[cfg(feature = "supabase")]
            ConnectorKind::Cloud(connector) => connector.revoke_session(bearer),
        }
    }

    pub async fn revoke_session_async(&self, bearer: &str) -> Result<(), DactylError> {
        match &self.inner {
            ConnectorKind::Local { .. } => {
                let _ = bearer;
                Err(local_cloud_capability_error())
            }
            #[cfg(feature = "supabase")]
            ConnectorKind::Cloud(connector) => connector.revoke_session_async(bearer).await,
        }
    }

    /// Invoke a trusted native JSON control function on this cloud connector.
    ///
    /// The function is one SQL identifier in the discovered API schema, with a
    /// single opaque `request` argument. No SQL, URL, caller identity label or
    /// policy interpretation is added. `bearer` must be the current managed user
    /// access token; `None` fails before dispatch. Local connectors return a
    /// capability error, never substitute local authority.
    ///
    /// `kind` classifies acknowledgement loss, not authorization or physical
    /// read-only enforcement. Use `Write` whenever the server might mutate,
    /// including audit writes or unknown behavior. `Read` is only for a verified
    /// side-effect-free operation. Dactyl never infers this from JSON actions.
    /// A lost mutation acknowledgement returns `outcome_unknown` and
    /// [`crate::MutationOutcome::Uncertain`]; reconcile rather than replay.
    ///
    /// The wire bytes and connect/total timeouts use `budget`; datastore row,
    /// SQL and cell limits do not apply to opaque JSON. JSON nesting is bounded.
    /// No automatic refresh, retry, redirect or durable session storage occurs.
    pub fn control_request(
        &self,
        function: &str,
        request: &serde_json::Value,
        bearer: Option<&str>,
        kind: OperationKind,
        budget: ResourceBudget,
    ) -> Result<serde_json::Value, DactylError> {
        let mut metadata = CloudResponseMetadata::default();
        self.control_request_with_metadata(
            function,
            request,
            bearer,
            None,
            kind,
            budget,
            &mut metadata,
        )
    }

    /// Invoke a trusted native JSON control function while capturing bounded
    /// response/challenge metadata (`DPoP-Nonce` and `Decapod-Cloud-*` headers),
    /// including when the server returns a `401` challenge or denial.
    #[allow(clippy::too_many_arguments)]
    pub fn control_request_with_metadata(
        &self,
        function: &str,
        request: &serde_json::Value,
        bearer: Option<&str>,
        dpop_nonce: Option<&str>,
        kind: OperationKind,
        budget: ResourceBudget,
        metadata_out: &mut CloudResponseMetadata,
    ) -> Result<serde_json::Value, DactylError> {
        match &self.inner {
            ConnectorKind::Local { .. } => {
                let _ = (
                    function,
                    request,
                    bearer,
                    dpop_nonce,
                    kind,
                    budget,
                    metadata_out,
                );
                Err(local_cloud_capability_error())
            }
            #[cfg(feature = "supabase")]
            ConnectorKind::Cloud(connector) => connector.control_request_with_metadata(
                function,
                request,
                bearer,
                dpop_nonce,
                kind,
                budget,
                metadata_out,
            ),
        }
    }

    /// Asynchronously invoke a trusted native JSON control function on this cloud connector.
    pub async fn control_request_async(
        &self,
        function: &str,
        request: &serde_json::Value,
        bearer: Option<&str>,
        kind: OperationKind,
        budget: ResourceBudget,
    ) -> Result<serde_json::Value, DactylError> {
        let mut metadata = CloudResponseMetadata::default();
        self.control_request_with_metadata_async(
            function,
            request,
            bearer,
            None,
            kind,
            budget,
            &mut metadata,
        )
        .await
    }

    /// Asynchronously invoke a trusted native JSON control function while capturing
    /// bounded response/challenge metadata and validated effect provenance.
    #[allow(clippy::too_many_arguments)]
    pub async fn control_request_with_metadata_async(
        &self,
        function: &str,
        request: &serde_json::Value,
        bearer: Option<&str>,
        dpop_nonce: Option<&str>,
        kind: OperationKind,
        budget: ResourceBudget,
        metadata_out: &mut CloudResponseMetadata,
    ) -> Result<serde_json::Value, DactylError> {
        match &self.inner {
            ConnectorKind::Local { .. } => {
                let _ = (
                    function,
                    request,
                    bearer,
                    dpop_nonce,
                    kind,
                    budget,
                    metadata_out,
                );
                Err(local_cloud_capability_error())
            }
            #[cfg(feature = "supabase")]
            ConnectorKind::Cloud(connector) => {
                connector
                    .control_request_with_metadata_async(
                        function,
                        request,
                        bearer,
                        dpop_nonce,
                        kind,
                        budget,
                        metadata_out,
                    )
                    .await
            }
        }
    }

    /// Invoke a trusted native JSON control function carrying caller-supplied
    /// attempt/idempotency identity and capturing bounded provenance.
    #[allow(clippy::too_many_arguments)]
    pub fn control_request_with_attempt(
        &self,
        function: &str,
        request: &serde_json::Value,
        bearer: Option<&str>,
        dpop_nonce: Option<&str>,
        kind: OperationKind,
        budget: ResourceBudget,
        attempt: &OperationAttempt,
        metadata_out: &mut CloudResponseMetadata,
    ) -> Result<serde_json::Value, DactylError> {
        match &self.inner {
            ConnectorKind::Local { .. } => {
                let _ = (
                    function,
                    request,
                    bearer,
                    dpop_nonce,
                    kind,
                    budget,
                    attempt,
                    metadata_out,
                );
                Err(local_cloud_capability_error())
            }
            #[cfg(feature = "supabase")]
            ConnectorKind::Cloud(connector) => connector.control_request_with_attempt(
                function,
                request,
                bearer,
                dpop_nonce,
                kind,
                budget,
                attempt,
                metadata_out,
            ),
        }
    }

    /// Asynchronously invoke a trusted native JSON control function carrying
    /// caller-supplied attempt/idempotency identity and capturing bounded provenance.
    #[allow(clippy::too_many_arguments)]
    pub async fn control_request_with_attempt_async(
        &self,
        function: &str,
        request: &serde_json::Value,
        bearer: Option<&str>,
        dpop_nonce: Option<&str>,
        kind: OperationKind,
        budget: ResourceBudget,
        attempt: &OperationAttempt,
        metadata_out: &mut CloudResponseMetadata,
    ) -> Result<serde_json::Value, DactylError> {
        match &self.inner {
            ConnectorKind::Local { .. } => {
                let _ = (
                    function,
                    request,
                    bearer,
                    dpop_nonce,
                    kind,
                    budget,
                    attempt,
                    metadata_out,
                );
                Err(local_cloud_capability_error())
            }
            #[cfg(feature = "supabase")]
            ConnectorKind::Cloud(connector) => {
                connector
                    .control_request_with_attempt_async(
                        function,
                        request,
                        bearer,
                        dpop_nonce,
                        kind,
                        budget,
                        attempt,
                        metadata_out,
                    )
                    .await
            }
        }
    }

    /// Transport a hosted account, device-enrollment, or billing request using
    /// the current user bearer. Customer membership, repository/project
    /// authority, seats, pricing, and entitlements stay on Decapod Cloud; its
    /// structured readiness and denial responses are returned without policy
    /// reproduction.
    pub fn account_request(
        &self,
        operation: AccountOperation,
        body: &serde_json::Value,
        bearer: &str,
        scm: Option<&ScmCredential>,
    ) -> Result<serde_json::Value, DactylError> {
        let mut metadata = CloudResponseMetadata::default();
        self.account_request_with_metadata(operation, body, bearer, scm, None, &mut metadata)
    }

    /// Transport a hosted account, device-enrollment, or billing request while
    /// capturing bounded response/challenge metadata (`DPoP-Nonce` and
    /// `Decapod-Cloud-*` headers), including on `401` replay/nonce challenges.
    pub fn account_request_with_metadata(
        &self,
        operation: AccountOperation,
        body: &serde_json::Value,
        bearer: &str,
        scm: Option<&ScmCredential>,
        dpop_nonce: Option<&str>,
        metadata_out: &mut CloudResponseMetadata,
    ) -> Result<serde_json::Value, DactylError> {
        match &self.inner {
            ConnectorKind::Local { .. } => {
                let _ = (operation, body, bearer, scm, dpop_nonce, metadata_out);
                Err(local_cloud_capability_error())
            }
            #[cfg(feature = "supabase")]
            ConnectorKind::Cloud(connector) => connector.account_request_with_metadata(
                operation,
                body,
                bearer,
                scm,
                dpop_nonce,
                metadata_out,
            ),
        }
    }

    /// Asynchronously transport a hosted account, device-enrollment, or billing request.
    pub async fn account_request_async(
        &self,
        operation: AccountOperation,
        body: &serde_json::Value,
        bearer: &str,
        scm: Option<&ScmCredential>,
    ) -> Result<serde_json::Value, DactylError> {
        let mut metadata = CloudResponseMetadata::default();
        self.account_request_with_metadata_async(operation, body, bearer, scm, None, &mut metadata)
            .await
    }

    /// Asynchronously transport a hosted account, device-enrollment, or billing
    /// request while capturing bounded response/challenge metadata and provenance.
    pub async fn account_request_with_metadata_async(
        &self,
        operation: AccountOperation,
        body: &serde_json::Value,
        bearer: &str,
        scm: Option<&ScmCredential>,
        dpop_nonce: Option<&str>,
        metadata_out: &mut CloudResponseMetadata,
    ) -> Result<serde_json::Value, DactylError> {
        match &self.inner {
            ConnectorKind::Local { .. } => {
                let _ = (operation, body, bearer, scm, dpop_nonce, metadata_out);
                Err(local_cloud_capability_error())
            }
            #[cfg(feature = "supabase")]
            ConnectorKind::Cloud(connector) => {
                connector
                    .account_request_with_metadata_async(
                        operation,
                        body,
                        bearer,
                        scm,
                        dpop_nonce,
                        metadata_out,
                    )
                    .await
            }
        }
    }

    /// Transport a hosted account, device-enrollment, or billing request carrying
    /// caller-supplied attempt/idempotency identity and capturing bounded provenance.
    #[allow(clippy::too_many_arguments)]
    pub fn account_request_with_attempt(
        &self,
        operation: AccountOperation,
        body: &serde_json::Value,
        bearer: &str,
        scm: Option<&ScmCredential>,
        dpop_nonce: Option<&str>,
        attempt: &OperationAttempt,
        metadata_out: &mut CloudResponseMetadata,
    ) -> Result<serde_json::Value, DactylError> {
        match &self.inner {
            ConnectorKind::Local { .. } => {
                let _ = (
                    operation,
                    body,
                    bearer,
                    scm,
                    dpop_nonce,
                    attempt,
                    metadata_out,
                );
                Err(local_cloud_capability_error())
            }
            #[cfg(feature = "supabase")]
            ConnectorKind::Cloud(connector) => connector.account_request_with_attempt(
                operation,
                body,
                bearer,
                scm,
                dpop_nonce,
                attempt,
                metadata_out,
            ),
        }
    }

    /// Asynchronously transport a hosted account, device-enrollment, or billing
    /// request carrying caller-supplied attempt/idempotency identity.
    #[allow(clippy::too_many_arguments)]
    pub async fn account_request_with_attempt_async(
        &self,
        operation: AccountOperation,
        body: &serde_json::Value,
        bearer: &str,
        scm: Option<&ScmCredential>,
        dpop_nonce: Option<&str>,
        attempt: &OperationAttempt,
        metadata_out: &mut CloudResponseMetadata,
    ) -> Result<serde_json::Value, DactylError> {
        match &self.inner {
            ConnectorKind::Local { .. } => {
                let _ = (
                    operation,
                    body,
                    bearer,
                    scm,
                    dpop_nonce,
                    attempt,
                    metadata_out,
                );
                Err(local_cloud_capability_error())
            }
            #[cfg(feature = "supabase")]
            ConnectorKind::Cloud(connector) => {
                connector
                    .account_request_with_attempt_async(
                        operation,
                        body,
                        bearer,
                        scm,
                        dpop_nonce,
                        attempt,
                        metadata_out,
                    )
                    .await
            }
        }
    }

    /// Execute a separate, non-mutating server readback to reconcile an earlier
    /// onboarding/gateway operation attempt without repeating the write.
    pub fn reconcile_operation(
        &self,
        attempt: &OperationAttempt,
        bearer: &str,
        dpop_nonce: Option<&str>,
        prior_client_status: Option<EffectStatus>,
        metadata_out: &mut CloudResponseMetadata,
    ) -> Result<ReconciliationObservation, DactylError> {
        match &self.inner {
            ConnectorKind::Local { .. } => {
                let _ = (
                    attempt,
                    bearer,
                    dpop_nonce,
                    prior_client_status,
                    metadata_out,
                );
                Err(local_cloud_capability_error())
            }
            #[cfg(feature = "supabase")]
            ConnectorKind::Cloud(connector) => connector.reconcile_operation(
                attempt,
                bearer,
                dpop_nonce,
                prior_client_status,
                metadata_out,
            ),
        }
    }

    /// Asynchronously execute a separate, non-mutating server readback to reconcile
    /// an earlier onboarding/gateway operation attempt without repeating the write.
    pub async fn reconcile_operation_async(
        &self,
        attempt: &OperationAttempt,
        bearer: &str,
        dpop_nonce: Option<&str>,
        prior_client_status: Option<EffectStatus>,
        metadata_out: &mut CloudResponseMetadata,
    ) -> Result<ReconciliationObservation, DactylError> {
        match &self.inner {
            ConnectorKind::Local { .. } => {
                let _ = (
                    attempt,
                    bearer,
                    dpop_nonce,
                    prior_client_status,
                    metadata_out,
                );
                Err(local_cloud_capability_error())
            }
            #[cfg(feature = "supabase")]
            ConnectorKind::Cloud(connector) => {
                connector
                    .reconcile_operation_async(
                        attempt,
                        bearer,
                        dpop_nonce,
                        prior_client_status,
                        metadata_out,
                    )
                    .await
            }
        }
    }

    /// Execute a separate, non-mutating control RPC readback to reconcile an earlier
    /// control operation attempt without repeating the write.
    #[allow(clippy::too_many_arguments)]
    pub fn reconcile_control_attempt(
        &self,
        function: &str,
        attempt: &OperationAttempt,
        bearer: Option<&str>,
        dpop_nonce: Option<&str>,
        budget: ResourceBudget,
        prior_client_status: Option<EffectStatus>,
        metadata_out: &mut CloudResponseMetadata,
    ) -> Result<ReconciliationObservation, DactylError> {
        match &self.inner {
            ConnectorKind::Local { .. } => {
                let _ = (
                    function,
                    attempt,
                    bearer,
                    dpop_nonce,
                    budget,
                    prior_client_status,
                    metadata_out,
                );
                Err(local_cloud_capability_error())
            }
            #[cfg(feature = "supabase")]
            ConnectorKind::Cloud(connector) => connector.reconcile_control_attempt(
                function,
                attempt,
                bearer,
                dpop_nonce,
                budget,
                prior_client_status,
                metadata_out,
            ),
        }
    }

    /// Asynchronously execute a separate, non-mutating control RPC readback to reconcile
    /// an earlier control operation attempt without repeating the write.
    #[allow(clippy::too_many_arguments)]
    pub async fn reconcile_control_attempt_async(
        &self,
        function: &str,
        attempt: &OperationAttempt,
        bearer: Option<&str>,
        dpop_nonce: Option<&str>,
        budget: ResourceBudget,
        prior_client_status: Option<EffectStatus>,
        metadata_out: &mut CloudResponseMetadata,
    ) -> Result<ReconciliationObservation, DactylError> {
        match &self.inner {
            ConnectorKind::Local { .. } => {
                let _ = (
                    function,
                    attempt,
                    bearer,
                    dpop_nonce,
                    budget,
                    prior_client_status,
                    metadata_out,
                );
                Err(local_cloud_capability_error())
            }
            #[cfg(feature = "supabase")]
            ConnectorKind::Cloud(connector) => {
                connector
                    .reconcile_control_attempt_async(
                        function,
                        attempt,
                        bearer,
                        dpop_nonce,
                        budget,
                        prior_client_status,
                        metadata_out,
                    )
                    .await
            }
        }
    }
}

fn local_cloud_capability_error() -> DactylError {
    DactylError::adapter_with_code(
        AdapterErrorKind::Capability,
        "cloud_connector_required",
        "managed cloud session and account operations require a cloud connector",
    )
}
