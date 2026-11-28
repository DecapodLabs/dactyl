//! Logical Decapod Cloud connector, public deployment discovery and stateless auth I/O.
//! The application owns browser interaction, key custody, and private session persistence.

use std::io::Read;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde::Deserialize;

use crate::budget::mark_remote_uncertain;
use crate::connector::{
    validate_canonical_path_segment, validate_dpop_nonce, validate_opaque_credential,
    AccountOperation, AuthScheme, AuthenticationSecrets, AuthenticationStart,
    CloudResponseMetadata, EffectProvenance, EffectStatus, ExecutionPhase, ObservationSource,
    OperationAttempt, ReconciliationObservation, RecoveryActionCategory, RequestProofSigner,
    ScmCredential, UserSession, DEFAULT_CLOUD_ORIGIN, FIRST_PARTY_AUTH_PATH,
    FIRST_PARTY_REPO_PATH_PREFIX,
};
use crate::http_pool::{
    acquire_client, allowlisted_cloud_code, bind_response_provenance, build_attempt_headers,
    build_auth_headers, canonical_url, cloud_error_kind_for_code, effective_auth_scheme,
    extract_error_recovery, extract_response_metadata, has_dpop_nonce_challenge, in_tokio_runtime,
    read_bounded_body_async, with_dpop_nonce_retry, with_dpop_nonce_retry_async, AsyncBodyError,
    AuthHeaders, HttpOutcome, ProvenanceCheckContext, SharedHttpClient,
};
use crate::{
    AdapterErrorKind, DactylError, DatastoreRoute, OperationKind, ResourceBudget, SupabaseRpcConfig,
};

const CANONICAL_ONBOARDING: &str = "/functions/v1/decapod-onboarding";
const LEGACY_ONBOARDING: &str = "/functions/v1/propodus-onboarding";
const BODY_LIMIT: u64 = 16 * 1024;
const MAX_JSON_DEPTH: usize = 64;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
enum OnboardingPrefix {
    #[default]
    Canonical,
    Legacy,
}

impl OnboardingPrefix {
    fn as_str(self) -> &'static str {
        match self {
            Self::Canonical => CANONICAL_ONBOARDING,
            Self::Legacy => LEGACY_ONBOARDING,
        }
    }
}

#[derive(Clone, Copy)]
struct RequestAuthPolicy<'a> {
    proof_signer: Option<&'a Arc<dyn RequestProofSigner>>,
    managed_bearer_proof: bool,
    require_dpop_proof: bool,
    require_human_bearer: bool,
}

impl<'a> RequestAuthPolicy<'a> {
    const fn none() -> Self {
        Self {
            proof_signer: None,
            managed_bearer_proof: false,
            require_dpop_proof: false,
            require_human_bearer: false,
        }
    }

    const fn human_bearer() -> Self {
        Self {
            proof_signer: None,
            managed_bearer_proof: false,
            require_dpop_proof: false,
            require_human_bearer: true,
        }
    }

    const fn optional(proof_signer: Option<&'a Arc<dyn RequestProofSigner>>) -> Self {
        Self {
            proof_signer,
            managed_bearer_proof: false,
            require_dpop_proof: false,
            require_human_bearer: false,
        }
    }
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct DeploymentProfile {
    version: u16,
    project_origin: String,
    publishable_key: String,
    schema: String,
}

#[derive(Clone)]
pub struct CloudConnector {
    config: SupabaseRpcConfig,
    onboarding_prefix: OnboardingPrefix,
    proof_signer: Option<Arc<dyn RequestProofSigner>>,
    dpop_nonce: Option<String>,
    http_budget: ResourceBudget,
    http_client: Arc<SharedHttpClient>,
}

impl std::fmt::Debug for CloudConnector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CloudConnector")
            .field("config", &self.config)
            .field("onboarding_prefix", &self.onboarding_prefix)
            .field("has_proof_signer", &self.proof_signer.is_some())
            .field("has_dpop_nonce", &self.dpop_nonce.is_some())
            .finish()
    }
}

impl CloudConnector {
    pub fn validate_origin(origin: &str) -> Result<(), DactylError> {
        validate_origin(origin).map(|_| ())
    }

    pub fn rpc_config(&self) -> &SupabaseRpcConfig {
        &self.config
    }

    /// Production uses one built-in origin. An explicit override supports
    /// controlled test/private deployments without changing project policy.
    /// Prefer [`crate::LogicalTarget::cloud`] or
    /// [`crate::LogicalTarget::cloud_with_origin`] for process-isolated selection.
    pub fn default_origin() -> String {
        std::env::var("DACTYL_CLOUD_ORIGIN")
            .ok()
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| DEFAULT_CLOUD_ORIGIN.into())
    }

    pub fn discover(origin: &str) -> Result<Self, DactylError> {
        Self::discover_with_metadata(origin).map(|(connector, _)| connector)
    }

    /// Asynchronously discover the version-1 Decapod Cloud deployment profile.
    pub async fn discover_async(origin: &str) -> Result<Self, DactylError> {
        Self::discover_with_metadata_async(origin)
            .await
            .map(|(connector, _)| connector)
    }

    /// Discover the version-1 Decapod Cloud deployment profile, returning bounded
    /// diagnostic/sunset response headers (`Decapod-Cloud-*`, `Sunset`, etc.).
    ///
    /// Probes `/functions/v1/decapod-onboarding/config` first and falls back once
    /// to `/functions/v1/propodus-onboarding/config` only on HTTP `404`.
    pub fn discover_with_metadata(
        origin: &str,
    ) -> Result<(Self, CloudResponseMetadata), DactylError> {
        validate_origin(origin)?;
        let http_budget = ResourceBudget::v1();
        let http_client = acquire_client(http_budget)?;
        let mut metadata = CloudResponseMetadata::default();
        let mut onboarding_prefix = OnboardingPrefix::Canonical;
        let profile: DeploymentProfile = match request(
            &http_client,
            http_budget,
            origin,
            None,
            "GET",
            &format!("{CANONICAL_ONBOARDING}/config"),
            None,
            None,
            None,
            RequestAuthPolicy::none(),
            None,
            None,
            Some(OperationKind::Read),
            ObservationSource::DirectResponse,
            false,
            &mut metadata,
        ) {
            Ok(profile) => profile,
            Err(error) if is_missing_onboarding_function(&error) => {
                onboarding_prefix = OnboardingPrefix::Legacy;
                metadata = CloudResponseMetadata::default();
                request(
                    &http_client,
                    http_budget,
                    origin,
                    None,
                    "GET",
                    &format!("{LEGACY_ONBOARDING}/config"),
                    None,
                    None,
                    None,
                    RequestAuthPolicy::none(),
                    None,
                    None,
                    Some(OperationKind::Read),
                    ObservationSource::DirectResponse,
                    false,
                    &mut metadata,
                )?
            }
            Err(error) => return Err(error),
        };
        if profile.version != 1
            || profile.project_origin.trim_end_matches('/') != origin.trim_end_matches('/')
        {
            return Err(protocol(
                "cloud deployment profile version or origin mismatch",
            ));
        }
        let config = SupabaseRpcConfig::v1(profile.project_origin, profile.publishable_key)
            .with_schema(profile.schema);
        crate::adapter::supabase::validate_rpc(&config)?;
        Ok((
            Self {
                config,
                onboarding_prefix,
                proof_signer: None,
                dpop_nonce: None,
                http_budget,
                http_client,
            },
            metadata,
        ))
    }

    /// Asynchronously discover the version-1 Decapod Cloud deployment profile.
    pub async fn discover_with_metadata_async(
        origin: &str,
    ) -> Result<(Self, CloudResponseMetadata), DactylError> {
        validate_origin(origin)?;
        let http_budget = ResourceBudget::v1();
        let http_client = acquire_client(http_budget)?;
        let mut metadata = CloudResponseMetadata::default();
        let mut onboarding_prefix = OnboardingPrefix::Canonical;
        let profile: DeploymentProfile = match request_async(
            &http_client,
            http_budget,
            origin,
            None,
            "GET",
            &format!("{CANONICAL_ONBOARDING}/config"),
            None,
            None,
            None,
            RequestAuthPolicy::none(),
            None,
            None,
            Some(OperationKind::Read),
            ObservationSource::DirectResponse,
            false,
            &mut metadata,
        )
        .await
        {
            Ok(profile) => profile,
            Err(error) if is_missing_onboarding_function(&error) => {
                onboarding_prefix = OnboardingPrefix::Legacy;
                metadata = CloudResponseMetadata::default();
                request_async(
                    &http_client,
                    http_budget,
                    origin,
                    None,
                    "GET",
                    &format!("{LEGACY_ONBOARDING}/config"),
                    None,
                    None,
                    None,
                    RequestAuthPolicy::none(),
                    None,
                    None,
                    Some(OperationKind::Read),
                    ObservationSource::DirectResponse,
                    false,
                    &mut metadata,
                )
                .await?
            }
            Err(error) => return Err(error),
        };
        if profile.version != 1
            || profile.project_origin.trim_end_matches('/') != origin.trim_end_matches('/')
        {
            return Err(protocol(
                "cloud deployment profile version or origin mismatch",
            ));
        }
        let config = SupabaseRpcConfig::v1(profile.project_origin, profile.publishable_key)
            .with_schema(profile.schema);
        crate::adapter::supabase::validate_rpc(&config)?;
        Ok((
            Self {
                config,
                onboarding_prefix,
                proof_signer: None,
                dpop_nonce: None,
                http_budget,
                http_client,
            },
            metadata,
        ))
    }

    /// Explicit seam for a trusted deployment profile or a disposable fixture.
    pub fn with_config(config: SupabaseRpcConfig) -> Result<Self, DactylError> {
        crate::adapter::supabase::validate_rpc(&config)?;
        let http_budget = ResourceBudget::v1();
        let http_client = acquire_client(http_budget)?;
        Ok(Self {
            config,
            onboarding_prefix: OnboardingPrefix::Canonical,
            proof_signer: None,
            dpop_nonce: None,
            http_budget,
            http_client,
        })
    }

    /// Select the connection timeout, concurrency, keep-alive, and request
    /// timeout settings for this connector's authentication and account requests.
    /// Control and datastore requests use their explicit per-call resource budgets.
    pub fn with_http_budget(mut self, budget: ResourceBudget) -> Result<Self, DactylError> {
        budget.validate()?;
        self.http_client = acquire_client(budget)?;
        self.http_budget = budget;
        Ok(self)
    }

    pub fn http_budget(&self) -> ResourceBudget {
        self.http_budget
    }

    /// Attach a synchronous caller-supplied [`RequestProofSigner`] for Decapod
    /// Cloud account, control RPC, and datastore requests.
    pub fn with_proof_signer(mut self, signer: Arc<dyn RequestProofSigner>) -> Self {
        self.proof_signer = Some(signer);
        self
    }

    /// Supply an initial server `DPoP-Nonce` challenge value for this connector.
    pub fn with_dpop_nonce(mut self, nonce: impl Into<String>) -> Self {
        self.dpop_nonce = Some(nonce.into());
        self
    }

    /// Route authenticated Decapod Cloud control and datastore RPCs through the
    /// Edge gateway endpoints (`/functions/v1/decapod-onboarding/cloud-*`) and
    /// enforce gateway-side Bearer/DPoP expectations per onboarding endpoint.
    pub fn with_device_gateway(mut self) -> Self {
        self.config = self.config.with_device_gateway();
        self
    }

    pub fn uses_device_gateway(&self) -> bool {
        self.config.uses_device_gateway()
    }

    pub fn from_runtime(origin: &str) -> Result<Self, DactylError> {
        // This setting is Dactyl-owned and machine-local, never project config.
        match std::env::var("DACTYL_CLOUD_PUBLISHABLE_KEY") {
            Ok(key) if !key.trim().is_empty() => {
                Self::with_config(SupabaseRpcConfig::v1(origin, key))
            }
            _ => Self::discover(origin),
        }
    }

    pub async fn from_runtime_async(origin: &str) -> Result<Self, DactylError> {
        match std::env::var("DACTYL_CLOUD_PUBLISHABLE_KEY") {
            Ok(key) if !key.trim().is_empty() => {
                Self::with_config(SupabaseRpcConfig::v1(origin, key))
            }
            _ => Self::discover_async(origin).await,
        }
    }

    pub fn origin(&self) -> &str {
        self.config.project_origin()
    }

    /// Build the canonical `/auth` application URL bound strictly to this
    /// connector's verified origin (`#138`).
    pub fn canonical_auth_url(&self) -> Result<String, DactylError> {
        let origin = validate_origin(self.origin())?;
        Ok(format!(
            "{}{FIRST_PARTY_AUTH_PATH}",
            origin.as_str().trim_end_matches('/')
        ))
    }

    /// Build the canonical `/decapod/<org>/<repo>` application URL bound
    /// strictly to this connector's verified origin (`#138`).
    pub fn canonical_repository_url(&self, org: &str, repo: &str) -> Result<String, DactylError> {
        let origin = validate_origin(self.origin())?;
        validate_canonical_path_segment(org, "invalid repository organization segment")?;
        validate_canonical_path_segment(repo, "invalid repository name segment")?;
        Ok(format!(
            "{}{FIRST_PARTY_REPO_PATH_PREFIX}/{org}/{repo}",
            origin.as_str().trim_end_matches('/')
        ))
    }

    pub fn route(&self, bearer: Option<String>) -> DatastoreRoute {
        let mut route = DatastoreRoute::supabase_rpc(self.config.clone(), bearer);
        if let Some(signer) = &self.proof_signer {
            route = route.with_proof_signer(signer.clone());
        }
        if let Some(nonce) = &self.dpop_nonce {
            route = route.with_dpop_nonce(nonce.clone());
        }
        route
    }

    /// Invoke an opaque native control function using the trusted deployment
    /// profile and the current managed user bearer. See
    /// [`crate::LogicalConnector::control_request`] for bounds and loss semantics.
    /// The caller selects a function identifier, never an origin or schema per
    /// request; server policy independently decides access and effective identity.
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

    /// Invoke an opaque native control function while capturing bounded
    /// response/challenge metadata (`DPoP-Nonce` and `Decapod-Cloud-*` headers).
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
        self.control_request_internal(
            function,
            request,
            bearer,
            dpop_nonce,
            kind,
            budget,
            None,
            ObservationSource::DirectResponse,
            metadata_out,
        )
    }

    /// Asynchronously invoke a trusted native JSON control function on this connector.
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

    /// Asynchronously invoke an opaque native control function while capturing
    /// bounded response/challenge metadata and validated provenance.
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
        self.control_request_internal_async(
            function,
            request,
            bearer,
            dpop_nonce,
            kind,
            budget,
            None,
            ObservationSource::DirectResponse,
            metadata_out,
        )
        .await
    }

    /// Invoke an opaque native control function carrying caller-supplied
    /// attempt/idempotency identity and capturing bounded effect provenance.
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
        self.control_request_internal(
            function,
            request,
            bearer,
            dpop_nonce,
            kind,
            budget,
            Some(attempt),
            ObservationSource::DirectResponse,
            metadata_out,
        )
    }

    /// Asynchronously invoke an opaque native control function carrying
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
        self.control_request_internal_async(
            function,
            request,
            bearer,
            dpop_nonce,
            kind,
            budget,
            Some(attempt),
            ObservationSource::DirectResponse,
            metadata_out,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    fn control_request_internal(
        &self,
        function: &str,
        request: &serde_json::Value,
        bearer: Option<&str>,
        dpop_nonce: Option<&str>,
        kind: OperationKind,
        budget: ResourceBudget,
        attempt: Option<&OperationAttempt>,
        observation_source: ObservationSource,
        metadata_out: &mut CloudResponseMetadata,
    ) -> Result<serde_json::Value, DactylError> {
        let primary_prefix = self.onboarding_prefix.as_str();
        let nonce = dpop_nonce.or(self.dpop_nonce.as_deref());
        match crate::adapter::supabase::control_request_with_auth_and_attempt(
            &self.config,
            primary_prefix,
            function,
            request,
            bearer,
            nonce,
            kind,
            budget,
            self.proof_signer.clone(),
            Some(self.http_client.clone()),
            attempt,
            observation_source,
            metadata_out,
        ) {
            Ok(value) => Ok(value),
            Err(error)
                if self.config.uses_device_gateway()
                    && self.onboarding_prefix == OnboardingPrefix::Canonical
                    && is_missing_onboarding_function(&error) =>
            {
                *metadata_out = CloudResponseMetadata::default();
                crate::adapter::supabase::control_request_with_auth_and_attempt(
                    &self.config,
                    LEGACY_ONBOARDING,
                    function,
                    request,
                    bearer,
                    nonce,
                    kind,
                    budget,
                    self.proof_signer.clone(),
                    Some(self.http_client.clone()),
                    attempt,
                    observation_source,
                    metadata_out,
                )
            }
            Err(error) => Err(error),
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn control_request_internal_async(
        &self,
        function: &str,
        request: &serde_json::Value,
        bearer: Option<&str>,
        dpop_nonce: Option<&str>,
        kind: OperationKind,
        budget: ResourceBudget,
        attempt: Option<&OperationAttempt>,
        observation_source: ObservationSource,
        metadata_out: &mut CloudResponseMetadata,
    ) -> Result<serde_json::Value, DactylError> {
        let primary_prefix = self.onboarding_prefix.as_str();
        let nonce = dpop_nonce.or(self.dpop_nonce.as_deref());
        match crate::adapter::supabase::control_request_with_auth_and_attempt_async(
            &self.config,
            primary_prefix,
            function,
            request,
            bearer,
            nonce,
            kind,
            budget,
            self.proof_signer.clone(),
            Some(self.http_client.clone()),
            attempt,
            observation_source,
            metadata_out,
        )
        .await
        {
            Ok(value) => Ok(value),
            Err(error)
                if self.config.uses_device_gateway()
                    && self.onboarding_prefix == OnboardingPrefix::Canonical
                    && is_missing_onboarding_function(&error) =>
            {
                *metadata_out = CloudResponseMetadata::default();
                crate::adapter::supabase::control_request_with_auth_and_attempt_async(
                    &self.config,
                    LEGACY_ONBOARDING,
                    function,
                    request,
                    bearer,
                    nonce,
                    kind,
                    budget,
                    self.proof_signer.clone(),
                    Some(self.http_client.clone()),
                    attempt,
                    observation_source,
                    metadata_out,
                )
                .await
            }
            Err(error) => Err(error),
        }
    }

    fn account_auth_policy(&self, operation: AccountOperation) -> RequestAuthPolicy<'_> {
        let require_human_bearer = operation.requires_human_bearer();
        if !self.config.uses_device_gateway() {
            return RequestAuthPolicy {
                proof_signer: self.proof_signer.as_ref(),
                managed_bearer_proof: false,
                require_dpop_proof: false,
                require_human_bearer,
            };
        }
        match operation {
            AccountOperation::DeviceEnroll | AccountOperation::DeviceSession => RequestAuthPolicy {
                proof_signer: self.proof_signer.as_ref(),
                managed_bearer_proof: true,
                require_dpop_proof: true,
                require_human_bearer: true,
            },
            AccountOperation::DeviceRefresh => RequestAuthPolicy {
                proof_signer: self.proof_signer.as_ref(),
                managed_bearer_proof: false,
                require_dpop_proof: true,
                require_human_bearer: false,
            },
            AccountOperation::DeviceRevoke => {
                RequestAuthPolicy::optional(self.proof_signer.as_ref())
            }
            AccountOperation::Register
            | AccountOperation::Status
            | AccountOperation::Quote
            | AccountOperation::Checkout
            | AccountOperation::Manage
            | AccountOperation::Portal
            | AccountOperation::BillingOveragePolicy
            | AccountOperation::RepositoryEnroll
            | AccountOperation::ResourceLimits
            | AccountOperation::OrgAdmins
            | AccountOperation::OrgRecovery
            | AccountOperation::PolicyTemplates
            | AccountOperation::OrgUpgrades
            | AccountOperation::DeviceChallenge
            | AccountOperation::DeviceKeys
            | AccountOperation::DeviceSessions
            | AccountOperation::DeviceStepUp => RequestAuthPolicy::human_bearer(),
        }
    }

    /// Transport an opaque versioned account, device-enrollment, billing, or
    /// administrative management request using the current user bearer. Optional
    /// SCM credentials are for explicit account linking only, never data or Auth
    /// token requests. Dactyl does not infer identity, roles, pricing or
    /// consent, including from a managed Auth provider token.
    ///
    /// Operations requiring a human session bearer reject `dpc_at_` device
    /// session tokens locally before dispatch. Mutating operations (`Register`,
    /// `Checkout`, `Manage`, `Portal`, `BillingOveragePolicy`,
    /// `RepositoryEnroll`, `OrgAdmins`, `OrgRecovery`, `PolicyTemplates`,
    /// `OrgUpgrades`, `DeviceEnroll`, `DeviceSession`, `DeviceRefresh`,
    /// `DeviceStepUp`, `DeviceRevoke`) attach error code `"outcome_unknown"` and
    /// [`crate::MutationOutcome::Uncertain`] if transport fails after dispatch
    /// so callers reconcile server state rather than blindly replaying a write.
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

    /// Transport an opaque versioned account, device-enrollment, or billing
    /// request while capturing bounded response/challenge metadata (`DPoP-Nonce`
    /// and `Decapod-Cloud-*` headers).
    pub fn account_request_with_metadata(
        &self,
        operation: AccountOperation,
        body: &serde_json::Value,
        bearer: &str,
        scm: Option<&ScmCredential>,
        dpop_nonce: Option<&str>,
        metadata_out: &mut CloudResponseMetadata,
    ) -> Result<serde_json::Value, DactylError> {
        let kind = if operation.is_mutating() {
            OperationKind::Write
        } else {
            OperationKind::Read
        };
        self.onboarding_request(
            operation.action(),
            Some(body.clone()),
            Some(bearer),
            scm,
            self.account_auth_policy(operation),
            dpop_nonce,
            None,
            Some(kind),
            ObservationSource::DirectResponse,
            operation.is_mutating(),
            metadata_out,
        )
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
        let kind = if operation.is_mutating() {
            OperationKind::Write
        } else {
            OperationKind::Read
        };
        self.onboarding_request_async(
            operation.action(),
            Some(body.clone()),
            Some(bearer),
            scm,
            self.account_auth_policy(operation),
            dpop_nonce,
            None,
            Some(kind),
            ObservationSource::DirectResponse,
            operation.is_mutating(),
            metadata_out,
        )
        .await
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
        let kind = if operation.is_mutating() {
            OperationKind::Write
        } else {
            OperationKind::Read
        };
        self.onboarding_request(
            operation.action(),
            Some(body.clone()),
            Some(bearer),
            scm,
            self.account_auth_policy(operation),
            dpop_nonce,
            Some(attempt),
            Some(kind),
            ObservationSource::DirectResponse,
            operation.is_mutating(),
            metadata_out,
        )
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
        let kind = if operation.is_mutating() {
            OperationKind::Write
        } else {
            OperationKind::Read
        };
        self.onboarding_request_async(
            operation.action(),
            Some(body.clone()),
            Some(bearer),
            scm,
            self.account_auth_policy(operation),
            dpop_nonce,
            Some(attempt),
            Some(kind),
            ObservationSource::DirectResponse,
            operation.is_mutating(),
            metadata_out,
        )
        .await
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
        let body = reconcile_body(attempt);
        let payload: serde_json::Value = self.onboarding_request(
            "operation-status",
            Some(body),
            Some(bearer),
            None,
            RequestAuthPolicy::optional(self.proof_signer.as_ref()),
            dpop_nonce,
            Some(attempt),
            Some(OperationKind::Read),
            ObservationSource::ServerReadback,
            false,
            metadata_out,
        )?;
        let provenance = metadata_out
            .provenance()
            .cloned()
            .ok_or_else(|| protocol("server reconciliation response omitted provenance state"))?;
        Ok(ReconciliationObservation::new(
            attempt.attempt_id().to_owned(),
            provenance
                .request_digest()
                .map(str::to_owned)
                .or_else(|| attempt.request_digest().map(str::to_owned)),
            provenance,
            prior_client_status,
            payload,
            metadata_out.clone(),
        ))
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
        let body = reconcile_body(attempt);
        let payload: serde_json::Value = self
            .onboarding_request_async(
                "operation-status",
                Some(body),
                Some(bearer),
                None,
                RequestAuthPolicy::optional(self.proof_signer.as_ref()),
                dpop_nonce,
                Some(attempt),
                Some(OperationKind::Read),
                ObservationSource::ServerReadback,
                false,
                metadata_out,
            )
            .await?;
        let provenance = metadata_out
            .provenance()
            .cloned()
            .ok_or_else(|| protocol("server reconciliation response omitted provenance state"))?;
        Ok(ReconciliationObservation::new(
            attempt.attempt_id().to_owned(),
            provenance
                .request_digest()
                .map(str::to_owned)
                .or_else(|| attempt.request_digest().map(str::to_owned)),
            provenance,
            prior_client_status,
            payload,
            metadata_out.clone(),
        ))
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
        let body = reconcile_body(attempt);
        let payload = self.control_request_internal(
            function,
            &body,
            bearer,
            dpop_nonce,
            OperationKind::Read,
            budget,
            Some(attempt),
            ObservationSource::ServerReadback,
            metadata_out,
        )?;
        let provenance = metadata_out
            .provenance()
            .cloned()
            .ok_or_else(|| protocol("server reconciliation response omitted provenance state"))?;
        Ok(ReconciliationObservation::new(
            attempt.attempt_id().to_owned(),
            provenance
                .request_digest()
                .map(str::to_owned)
                .or_else(|| attempt.request_digest().map(str::to_owned)),
            provenance,
            prior_client_status,
            payload,
            metadata_out.clone(),
        ))
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
        let body = reconcile_body(attempt);
        let payload = self
            .control_request_internal_async(
                function,
                &body,
                bearer,
                dpop_nonce,
                OperationKind::Read,
                budget,
                Some(attempt),
                ObservationSource::ServerReadback,
                metadata_out,
            )
            .await?;
        let provenance = metadata_out
            .provenance()
            .cloned()
            .ok_or_else(|| protocol("server reconciliation response omitted provenance state"))?;
        Ok(ReconciliationObservation::new(
            attempt.attempt_id().to_owned(),
            provenance
                .request_digest()
                .map(str::to_owned)
                .or_else(|| attempt.request_digest().map(str::to_owned)),
            provenance,
            prior_client_status,
            payload,
            metadata_out.clone(),
        ))
    }

    pub fn start_authentication(
        &self,
        repository: &str,
        secrets: &AuthenticationSecrets,
    ) -> Result<AuthenticationStart, DactylError> {
        self.start_authentication_with_purpose(repository, secrets, None)
    }

    /// Start managed authentication with an optional caller-declared purpose.
    /// The service interprets the purpose and decides the resulting handoff.
    pub fn start_authentication_with_purpose(
        &self,
        repository: &str,
        secrets: &AuthenticationSecrets,
        purpose: Option<&str>,
    ) -> Result<AuthenticationStart, DactylError> {
        let body = authentication_start_body(repository, secrets, purpose)?;
        let response: AuthenticationStart = self.auth_request("start", &body, None, false)?;
        self.validate_start(&response)?;
        Ok(response)
    }

    pub async fn start_authentication_async(
        &self,
        repository: &str,
        secrets: &AuthenticationSecrets,
    ) -> Result<AuthenticationStart, DactylError> {
        self.start_authentication_with_purpose_async(repository, secrets, None)
            .await
    }

    /// Asynchronous counterpart to [Self::start_authentication_with_purpose].
    pub async fn start_authentication_with_purpose_async(
        &self,
        repository: &str,
        secrets: &AuthenticationSecrets,
        purpose: Option<&str>,
    ) -> Result<AuthenticationStart, DactylError> {
        let body = authentication_start_body(repository, secrets, purpose)?;
        let response: AuthenticationStart =
            self.auth_request_async("start", &body, None, false).await?;
        self.validate_start(&response)?;
        Ok(response)
    }

    pub fn validate_start(&self, flow: &AuthenticationStart) -> Result<(), DactylError> {
        validate_flow_id(&flow.flow_id)?;
        let browser = reqwest::Url::parse(&flow.browser_url)
            .map_err(|_| protocol("invalid authentication handoff"))?;
        let origin = validate_origin(self.origin())?;
        if browser.origin() != origin.origin()
            || !(browser.path() == format!("{CANONICAL_ONBOARDING}/begin")
                || browser.path() == format!("{LEGACY_ONBOARDING}/begin"))
            || !browser.username().is_empty()
            || browser.password().is_some()
            || browser.fragment().is_some()
            || browser
                .query_pairs()
                .filter(|(k, v)| k == "flow_id" && v == flow.flow_id.as_str())
                .count()
                != 1
            || browser
                .query_pairs()
                .filter(|(k, _)| k == "flow_id")
                .count()
                != 1
            || browser
                .query_pairs()
                .filter(|(k, v)| k == "ticket" && !v.is_empty())
                .count()
                != 1
            || browser.query_pairs().filter(|(k, _)| k == "ticket").count() != 1
        {
            return Err(protocol(
                "authentication handoff must remain on the selected cloud origin",
            ));
        }
        Ok(())
    }

    pub fn authentication_status(
        &self,
        flow_id: &str,
        secrets: &AuthenticationSecrets,
    ) -> Result<String, DactylError> {
        let response: serde_json::Value =
            self.auth_request("status", &flow_body(flow_id, secrets)?, None, false)?;
        parse_auth_status(&response)
    }

    pub async fn authentication_status_async(
        &self,
        flow_id: &str,
        secrets: &AuthenticationSecrets,
    ) -> Result<String, DactylError> {
        let response: serde_json::Value = self
            .auth_request_async("status", &flow_body(flow_id, secrets)?, None, false)
            .await?;
        parse_auth_status(&response)
    }

    /// One-time operation. The caller must persist an in-flight marker before
    /// calling; a lost response returns code `"outcome_unknown"` and cannot be
    /// retried as if it never committed.
    pub fn exchange_authentication_code(
        &self,
        flow_id: &str,
        secrets: &AuthenticationSecrets,
    ) -> Result<String, DactylError> {
        let response: serde_json::Value =
            self.auth_request("exchange", &flow_body(flow_id, secrets)?, None, true)?;
        parse_auth_exchange(&response)
    }

    pub async fn exchange_authentication_code_async(
        &self,
        flow_id: &str,
        secrets: &AuthenticationSecrets,
    ) -> Result<String, DactylError> {
        let response: serde_json::Value = self
            .auth_request_async("exchange", &flow_body(flow_id, secrets)?, None, true)
            .await?;
        parse_auth_exchange(&response)
    }

    pub fn exchange_session(
        &self,
        code: &str,
        secrets: &AuthenticationSecrets,
    ) -> Result<UserSession, DactylError> {
        secrets.validate()?;
        opaque(code)?;
        self.token_request(
            "pkce",
            serde_json::json!({"auth_code": code, "code_verifier": secrets.verifier}),
        )
    }

    pub async fn exchange_session_async(
        &self,
        code: &str,
        secrets: &AuthenticationSecrets,
    ) -> Result<UserSession, DactylError> {
        secrets.validate()?;
        opaque(code)?;
        self.token_request_async(
            "pkce",
            serde_json::json!({"auth_code": code, "code_verifier": secrets.verifier}),
        )
        .await
    }

    pub fn refresh_session(&self, refresh_token: &str) -> Result<UserSession, DactylError> {
        opaque(refresh_token)?;
        self.token_request(
            "refresh_token",
            serde_json::json!({"refresh_token": refresh_token}),
        )
    }

    pub async fn refresh_session_async(
        &self,
        refresh_token: &str,
    ) -> Result<UserSession, DactylError> {
        opaque(refresh_token)?;
        self.token_request_async(
            "refresh_token",
            serde_json::json!({"refresh_token": refresh_token}),
        )
        .await
    }

    pub fn complete_authentication(
        &self,
        flow_id: &str,
        secrets: &AuthenticationSecrets,
        bearer: &str,
    ) -> Result<(), DactylError> {
        let result: serde_json::Value = self.auth_request(
            "complete",
            &flow_body(flow_id, secrets)?,
            Some(bearer),
            true,
        )?;
        parse_auth_complete(&result)
    }

    pub async fn complete_authentication_async(
        &self,
        flow_id: &str,
        secrets: &AuthenticationSecrets,
        bearer: &str,
    ) -> Result<(), DactylError> {
        let result: serde_json::Value = self
            .auth_request_async(
                "complete",
                &flow_body(flow_id, secrets)?,
                Some(bearer),
                true,
            )
            .await?;
        parse_auth_complete(&result)
    }

    pub fn revoke_session(&self, bearer: &str) -> Result<(), DactylError> {
        let mut metadata = CloudResponseMetadata::default();
        let _: serde_json::Value = request(
            &self.http_client,
            self.http_budget,
            self.origin(),
            Some(self.config.publishable_key()),
            "POST",
            "/auth/v1/logout?scope=local",
            None,
            Some(bearer),
            None,
            RequestAuthPolicy::optional(self.proof_signer.as_ref()),
            self.dpop_nonce.as_deref(),
            None,
            Some(OperationKind::Write),
            ObservationSource::DirectResponse,
            true,
            &mut metadata,
        )?;
        Ok(())
    }

    pub async fn revoke_session_async(&self, bearer: &str) -> Result<(), DactylError> {
        let mut metadata = CloudResponseMetadata::default();
        let _: serde_json::Value = request_async(
            &self.http_client,
            self.http_budget,
            self.origin(),
            Some(self.config.publishable_key()),
            "POST",
            "/auth/v1/logout?scope=local",
            None,
            Some(bearer),
            None,
            RequestAuthPolicy::optional(self.proof_signer.as_ref()),
            self.dpop_nonce.as_deref(),
            None,
            Some(OperationKind::Write),
            ObservationSource::DirectResponse,
            true,
            &mut metadata,
        )
        .await?;
        Ok(())
    }

    fn token_request(
        &self,
        grant: &str,
        body: serde_json::Value,
    ) -> Result<UserSession, DactylError> {
        let mut metadata = CloudResponseMetadata::default();
        let response: TokenResponse = request(
            &self.http_client,
            self.http_budget,
            self.origin(),
            Some(self.config.publishable_key()),
            "POST",
            &format!("/auth/v1/token?grant_type={grant}"),
            Some(body),
            None,
            None,
            RequestAuthPolicy::none(),
            None,
            None,
            Some(OperationKind::Write),
            ObservationSource::DirectResponse,
            true,
            &mut metadata,
        )?;
        finalize_token_response(response)
    }

    async fn token_request_async(
        &self,
        grant: &str,
        body: serde_json::Value,
    ) -> Result<UserSession, DactylError> {
        let mut metadata = CloudResponseMetadata::default();
        let response: TokenResponse = request_async(
            &self.http_client,
            self.http_budget,
            self.origin(),
            Some(self.config.publishable_key()),
            "POST",
            &format!("/auth/v1/token?grant_type={grant}"),
            Some(body),
            None,
            None,
            RequestAuthPolicy::none(),
            None,
            None,
            Some(OperationKind::Write),
            ObservationSource::DirectResponse,
            true,
            &mut metadata,
        )
        .await?;
        finalize_token_response(response)
    }

    fn auth_request<T: for<'de> Deserialize<'de> + Send + 'static>(
        &self,
        action: &str,
        body: &serde_json::Value,
        bearer: Option<&str>,
        is_mutating: bool,
    ) -> Result<T, DactylError> {
        let mut metadata = CloudResponseMetadata::default();
        let kind = if is_mutating {
            OperationKind::Write
        } else {
            OperationKind::Read
        };
        self.onboarding_request(
            action,
            Some(body.clone()),
            bearer,
            None,
            RequestAuthPolicy::none(),
            None,
            None,
            Some(kind),
            ObservationSource::DirectResponse,
            is_mutating,
            &mut metadata,
        )
    }

    async fn auth_request_async<T: for<'de> Deserialize<'de> + Send + 'static>(
        &self,
        action: &str,
        body: &serde_json::Value,
        bearer: Option<&str>,
        is_mutating: bool,
    ) -> Result<T, DactylError> {
        let mut metadata = CloudResponseMetadata::default();
        let kind = if is_mutating {
            OperationKind::Write
        } else {
            OperationKind::Read
        };
        self.onboarding_request_async(
            action,
            Some(body.clone()),
            bearer,
            None,
            RequestAuthPolicy::none(),
            None,
            None,
            Some(kind),
            ObservationSource::DirectResponse,
            is_mutating,
            &mut metadata,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    fn onboarding_request<T: for<'de> Deserialize<'de> + Send + 'static>(
        &self,
        action: &str,
        body: Option<serde_json::Value>,
        bearer: Option<&str>,
        scm: Option<&ScmCredential>,
        auth_policy: RequestAuthPolicy<'_>,
        dpop_nonce: Option<&str>,
        attempt: Option<&OperationAttempt>,
        operation_kind: Option<OperationKind>,
        observation_source: ObservationSource,
        is_mutating: bool,
        metadata_out: &mut CloudResponseMetadata,
    ) -> Result<T, DactylError> {
        let primary_prefix = self.onboarding_prefix.as_str();
        let nonce = dpop_nonce.or(self.dpop_nonce.as_deref());
        match request(
            &self.http_client,
            self.http_budget,
            self.origin(),
            Some(self.config.publishable_key()),
            "POST",
            &format!("{primary_prefix}/{action}"),
            body.clone(),
            bearer,
            scm,
            auth_policy,
            nonce,
            attempt,
            operation_kind,
            observation_source,
            is_mutating,
            metadata_out,
        ) {
            Ok(value) => Ok(value),
            Err(error)
                if self.onboarding_prefix == OnboardingPrefix::Canonical
                    && is_missing_onboarding_function(&error) =>
            {
                *metadata_out = CloudResponseMetadata::default();
                request(
                    &self.http_client,
                    self.http_budget,
                    self.origin(),
                    Some(self.config.publishable_key()),
                    "POST",
                    &format!("{LEGACY_ONBOARDING}/{action}"),
                    body,
                    bearer,
                    scm,
                    auth_policy,
                    nonce,
                    attempt,
                    operation_kind,
                    observation_source,
                    is_mutating,
                    metadata_out,
                )
            }
            Err(error) => Err(error),
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn onboarding_request_async<T: for<'de> Deserialize<'de> + Send + 'static>(
        &self,
        action: &str,
        body: Option<serde_json::Value>,
        bearer: Option<&str>,
        scm: Option<&ScmCredential>,
        auth_policy: RequestAuthPolicy<'_>,
        dpop_nonce: Option<&str>,
        attempt: Option<&OperationAttempt>,
        operation_kind: Option<OperationKind>,
        observation_source: ObservationSource,
        is_mutating: bool,
        metadata_out: &mut CloudResponseMetadata,
    ) -> Result<T, DactylError> {
        let primary_prefix = self.onboarding_prefix.as_str();
        let nonce = dpop_nonce.or(self.dpop_nonce.as_deref());
        match request_async(
            &self.http_client,
            self.http_budget,
            self.origin(),
            Some(self.config.publishable_key()),
            "POST",
            &format!("{primary_prefix}/{action}"),
            body.clone(),
            bearer,
            scm,
            auth_policy,
            nonce,
            attempt,
            operation_kind,
            observation_source,
            is_mutating,
            metadata_out,
        )
        .await
        {
            Ok(value) => Ok(value),
            Err(error)
                if self.onboarding_prefix == OnboardingPrefix::Canonical
                    && is_missing_onboarding_function(&error) =>
            {
                *metadata_out = CloudResponseMetadata::default();
                request_async(
                    &self.http_client,
                    self.http_budget,
                    self.origin(),
                    Some(self.config.publishable_key()),
                    "POST",
                    &format!("{LEGACY_ONBOARDING}/{action}"),
                    body,
                    bearer,
                    scm,
                    auth_policy,
                    nonce,
                    attempt,
                    operation_kind,
                    observation_source,
                    is_mutating,
                    metadata_out,
                )
                .await
            }
            Err(error) => Err(error),
        }
    }
}

#[derive(Deserialize)]
struct TokenResponse {
    access_token: String,
    refresh_token: String,
    expires_in: u64,
    #[serde(default)]
    provider_token: Option<String>,
}

fn authentication_start_body(
    repository: &str,
    secrets: &AuthenticationSecrets,
    purpose: Option<&str>,
) -> Result<serde_json::Value, DactylError> {
    secrets.validate()?;
    let mut body = serde_json::json!({
        "version": 1,
        "repository": repository,
        "pkce_challenge": AuthenticationSecrets::challenge(&secrets.verifier),
        "poll_challenge": AuthenticationSecrets::challenge(&secrets.poll_secret)
    });
    if let Some(purpose) = purpose {
        if purpose.is_empty()
            || purpose.len() > 64
            || !purpose
                .bytes()
                .all(|ch| ch.is_ascii_lowercase() || ch == b'_')
        {
            return Err(protocol("invalid authentication purpose"));
        }
        body["purpose"] = purpose.into();
    }
    Ok(body)
}

fn finalize_token_response(response: TokenResponse) -> Result<UserSession, DactylError> {
    opaque(&response.access_token)?;
    opaque(&response.refresh_token)?;
    if let Some(token) = response.provider_token.as_deref() {
        opaque(token)?;
    }
    if response.expires_in == 0 || response.expires_in > 86400 {
        return Err(protocol("invalid managed session lifetime"));
    }
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| protocol("invalid system time"))?
        .as_secs();
    Ok(UserSession {
        access_token: response.access_token,
        refresh_token: response.refresh_token,
        expires_at_unix: now + response.expires_in,
        provider_token: response.provider_token,
    })
}

fn parse_auth_status(response: &serde_json::Value) -> Result<String, DactylError> {
    match response.get("status").and_then(|v| v.as_str()) {
        Some(status @ ("pending" | "ready" | "consumed" | "complete")) => Ok(status.into()),
        _ => Err(protocol("invalid authentication status")),
    }
}

fn parse_auth_exchange(response: &serde_json::Value) -> Result<String, DactylError> {
    let code = response
        .get("auth_code")
        .and_then(|v| v.as_str())
        .ok_or_else(|| {
            DactylError::adapter_with_code(
                AdapterErrorKind::Protocol,
                "outcome_unknown",
                "authentication exchange returned no code",
            )
        })?;
    opaque(code)?;
    Ok(code.into())
}

fn parse_auth_complete(result: &serde_json::Value) -> Result<(), DactylError> {
    if result.get("status").and_then(|v| v.as_str()) != Some("complete") {
        return Err(DactylError::adapter_with_code(
            AdapterErrorKind::Protocol,
            "outcome_unknown",
            "authentication completion was not confirmed",
        ));
    }
    Ok(())
}

fn reconcile_body(attempt: &OperationAttempt) -> serde_json::Value {
    let mut body = serde_json::json!({
        "version": 1,
        "attempt_id": attempt.attempt_id(),
    });
    if let Some(digest) = attempt.request_digest() {
        body["request_digest"] = serde_json::Value::String(digest.to_owned());
    }
    if let Some(scope) = attempt.scope_ref() {
        body["scope_ref"] = serde_json::Value::String(scope.to_owned());
    }
    body
}

fn flow_body(
    flow_id: &str,
    secrets: &AuthenticationSecrets,
) -> Result<serde_json::Value, DactylError> {
    secrets.validate()?;
    validate_flow_id(flow_id)?;
    Ok(serde_json::json!({"version": 1, "flow_id": flow_id, "poll_secret": secrets.poll_secret}))
}

fn validate_flow_id(value: &str) -> Result<(), DactylError> {
    if value.len() != 36
        || !value.bytes().enumerate().all(|(i, b)| {
            if [8, 13, 18, 23].contains(&i) {
                b == b'-'
            } else {
                b.is_ascii_hexdigit()
            }
        })
    {
        return Err(protocol("invalid authentication flow identifier"));
    }
    Ok(())
}

fn opaque(value: &str) -> Result<(), DactylError> {
    validate_opaque_credential(value, "invalid authentication credential")
}

fn validate_origin(origin: &str) -> Result<reqwest::Url, DactylError> {
    let url = crate::adapter::supabase::validate_endpoint(origin)?;
    if url.path() != "/" {
        return Err(protocol("cloud requires a project origin without a path"));
    }
    Ok(url)
}

fn protocol(message: &'static str) -> DactylError {
    DactylError::adapter(AdapterErrorKind::Protocol, message)
}

fn is_missing_onboarding_function(error: &DactylError) -> bool {
    error.adapter_kind() == Some(AdapterErrorKind::NotFound)
        && matches!(error.adapter_code(), None | Some("not_found"))
        && error.mutation_outcome() != crate::MutationOutcome::Uncertain
}

fn post_send_error(
    kind: AdapterErrorKind,
    is_mutating: bool,
    message: &'static str,
) -> DactylError {
    if is_mutating {
        mark_remote_uncertain(DactylError::adapter_with_code(
            kind,
            "outcome_unknown",
            message,
        ))
    } else {
        DactylError::adapter(kind, message)
    }
}

fn check_json_depth(bytes: &[u8]) -> bool {
    let mut depth = 0_usize;
    let mut in_string = false;
    let mut escaped = false;
    for &byte in bytes {
        if in_string {
            if escaped {
                escaped = false;
            } else if byte == b'\\' {
                escaped = true;
            } else if byte == b'"' {
                in_string = false;
            }
            continue;
        }
        match byte {
            b'"' => in_string = true,
            b'{' | b'[' => {
                depth += 1;
                if depth > MAX_JSON_DEPTH {
                    return false;
                }
            }
            b'}' | b']' => {
                depth = depth.saturating_sub(1);
            }
            _ => {}
        }
    }
    true
}

fn public_error_code(envelope: &serde_json::Value) -> Option<&'static str> {
    let raw = envelope
        .get("error")
        .and_then(|v| v.get("code"))
        .and_then(|v| v.as_str())
        .or_else(|| envelope.get("error_code").and_then(|v| v.as_str()))
        .or_else(|| envelope.get("code").and_then(|v| v.as_str()))
        .or_else(|| envelope.get("error").and_then(|v| v.as_str()))?;
    allowlisted_cloud_code(raw)
}

struct PreparedCloudRequest {
    expected_origin: reqwest::Url,
    endpoint: String,
    canonical: String,
    method_str: String,
    method: reqwest::Method,
    key: Option<reqwest::header::HeaderValue>,
    auth_header: Option<reqwest::header::HeaderValue>,
    dpop_header: Option<reqwest::header::HeaderValue>,
    scm: Option<(&'static str, &'static str, reqwest::header::HeaderValue)>,
    body: Option<Vec<u8>>,
    attempt: Option<OperationAttempt>,
    resolved_digest: Option<String>,
    attempt_headers: Vec<(&'static str, reqwest::header::HeaderValue)>,
    operation_kind: Option<OperationKind>,
    observation_source: ObservationSource,
    is_mutating: bool,
    request_timeout: Duration,
}

#[allow(clippy::too_many_arguments)]
fn prepare_cloud_request(
    origin: &str,
    key: Option<&str>,
    method_name: &str,
    path: &str,
    body: Option<serde_json::Value>,
    bearer: Option<&str>,
    scm: Option<&ScmCredential>,
    auth_policy: RequestAuthPolicy<'_>,
    dpop_nonce: Option<&str>,
    attempt: Option<&OperationAttempt>,
    operation_kind: Option<OperationKind>,
    observation_source: ObservationSource,
    is_mutating: bool,
    http_budget: ResourceBudget,
    metadata_out: &mut CloudResponseMetadata,
) -> Result<PreparedCloudRequest, DactylError> {
    let record_preflight = |err: DactylError,
                            metadata_out: &mut CloudResponseMetadata,
                            auth_scheme: Option<AuthScheme>| {
        let err = match err {
            DactylError::Adapter {
                kind,
                code,
                message,
            } if !message.starts_with("local preflight:")
                && !message.starts_with(crate::budget::LOCAL_PREFLIGHT_PREFIX) =>
            {
                DactylError::Adapter {
                    kind,
                    code,
                    message: format!("local preflight: {message}"),
                }
            }
            other => other,
        };
        metadata_out.set_provenance(EffectProvenance::local_preflight(
            operation_kind,
            attempt,
            err.adapter_code(),
            auth_scheme,
        ));
        err
    };

    let expected_origin = match validate_origin(origin) {
        Ok(url) => url,
        Err(err) => return Err(record_preflight(err, metadata_out, None)),
    };
    let endpoint = format!("{}{path}", origin.trim_end_matches('/'));
    let canonical = match canonical_url(&endpoint) {
        Ok(c) => c,
        Err(err) => return Err(record_preflight(err, metadata_out, None)),
    };
    let method_str = method_name.to_owned();
    let method = match reqwest::Method::from_bytes(method_name.as_bytes()) {
        Ok(m) => m,
        Err(_) => {
            return Err(record_preflight(
                protocol("invalid auth request method"),
                metadata_out,
                None,
            ))
        }
    };
    let header = |value: &str| -> Result<reqwest::header::HeaderValue, DactylError> {
        let mut header = reqwest::header::HeaderValue::from_str(value)
            .map_err(|_| protocol("invalid authentication header"))?;
        header.set_sensitive(true);
        Ok(header)
    };
    let key = match key.map(header).transpose() {
        Ok(k) => k,
        Err(err) => return Err(record_preflight(err, metadata_out, None)),
    };
    if let Some(nonce) = dpop_nonce {
        if let Err(err) = validate_dpop_nonce(nonce) {
            return Err(record_preflight(err, metadata_out, None));
        }
    }
    let (auth_header, dpop_header) = match bearer {
        Some(value) => {
            if let Err(err) = opaque(value) {
                return Err(record_preflight(err, metadata_out, None));
            }
            if auth_policy.require_human_bearer && value.starts_with("dpc_at_") {
                return Err(record_preflight(
                    DactylError::adapter_with_code(
                        AdapterErrorKind::Authentication,
                        "authentication_required",
                        "local preflight: operation requires a human bearer session, not a device session token",
                    ),
                    metadata_out,
                    Some(AuthScheme::Bearer),
                ));
            }
            let (auth, dpop) = match build_auth_headers(
                &method_str,
                &canonical,
                value,
                auth_policy.proof_signer,
                dpop_nonce,
            ) {
                Ok(pair) => pair,
                Err(err) => return Err(record_preflight(err, metadata_out, None)),
            };
            if auth_policy.require_dpop_proof && dpop.is_none() {
                return Err(record_preflight(
                    DactylError::adapter_with_code(
                        AdapterErrorKind::Authentication,
                        "device_proof_required",
                        "local preflight: operation requires a DPoP proof signer",
                    ),
                    metadata_out,
                    Some(AuthScheme::Bearer),
                ));
            }
            if auth_policy.managed_bearer_proof {
                let bearer_hdr = match header(&format!("Bearer {value}")) {
                    Ok(h) => h,
                    Err(err) => return Err(record_preflight(err, metadata_out, None)),
                };
                (Some(bearer_hdr), dpop)
            } else {
                (Some(auth), dpop)
            }
        }
        None => {
            if auth_policy.require_dpop_proof
                || auth_policy.managed_bearer_proof
                || auth_policy.require_human_bearer
            {
                return Err(record_preflight(
                    DactylError::adapter_with_code(
                        AdapterErrorKind::Authentication,
                        "authentication_required",
                        "local preflight: operation requires authentication",
                    ),
                    metadata_out,
                    None,
                ));
            }
            (None, None)
        }
    };
    let auth_scheme = effective_auth_scheme(&(auth_header.clone(), dpop_header.clone()));
    let scm = match scm
        .map(|credential| {
            Ok::<_, DactylError>((
                credential.provider().label(),
                credential.host(),
                header(credential.token())?,
            ))
        })
        .transpose()
    {
        Ok(scm) => scm,
        Err(err) => return Err(record_preflight(err, metadata_out, auth_scheme)),
    };
    let body = match body
        .map(|v| serde_json::to_vec(&v).map_err(|_| protocol("invalid auth request")))
        .transpose()
    {
        Ok(body) => body,
        Err(err) => return Err(record_preflight(err, metadata_out, auth_scheme)),
    };
    if body.as_ref().is_some_and(|v| v.len() as u64 > BODY_LIMIT) {
        return Err(record_preflight(
            protocol("auth request exceeds body limit"),
            metadata_out,
            auth_scheme,
        ));
    }
    let body_for_digest = if observation_source == ObservationSource::ServerReadback {
        None
    } else {
        body.as_deref()
    };
    let (resolved_digest, attempt_headers) =
        match build_attempt_headers(attempt, body_for_digest, operation_kind) {
            Ok(pair) => pair,
            Err(err) => return Err(record_preflight(err, metadata_out, auth_scheme)),
        };

    Ok(PreparedCloudRequest {
        expected_origin,
        endpoint,
        canonical,
        method_str,
        method,
        key,
        auth_header,
        dpop_header,
        scm,
        body,
        attempt: attempt.cloned(),
        resolved_digest,
        attempt_headers,
        operation_kind,
        observation_source,
        is_mutating,
        request_timeout: http_budget.request_timeout(),
    })
}

fn error_kind_for_cloud_status_and_code(status: u16, code: Option<&str>) -> AdapterErrorKind {
    if let Some(kind) = code.and_then(|c| cloud_error_kind_for_code(status, c)) {
        return kind;
    }
    match status {
        400 | 401 => AdapterErrorKind::Authentication,
        402 | 403 => AdapterErrorKind::Authorization,
        404 => AdapterErrorKind::NotFound,
        409 => AdapterErrorKind::Conflict,
        413 => AdapterErrorKind::Quota,
        429 => AdapterErrorKind::RateLimited,
        500..=599 => AdapterErrorKind::Unavailable,
        _ => AdapterErrorKind::Protocol,
    }
}

#[allow(clippy::too_many_arguments)]
fn handle_cloud_response<T: for<'de> Deserialize<'de>>(
    prepared: &PreparedCloudRequest,
    auth: &AuthHeaders,
    status: reqwest::StatusCode,
    headers: &reqwest::header::HeaderMap,
    response_url: &reqwest::Url,
    bytes: &[u8],
    mut metadata: CloudResponseMetadata,
) -> HttpOutcome<T> {
    let status_u16 = status.as_u16();
    let dpop_challenge =
        status == reqwest::StatusCode::UNAUTHORIZED && has_dpop_nonce_challenge(headers);
    let auth_scheme = effective_auth_scheme(auth);
    let prov_ctx = ProvenanceCheckContext {
        expected_origin: &prepared.expected_origin,
        response_url,
        expected_auth: auth_scheme,
        expected_kind: prepared.operation_kind,
        attempt: prepared.attempt.as_ref(),
        resolved_digest: prepared.resolved_digest.as_deref(),
        observation_source: prepared.observation_source,
        http_status: status_u16,
        is_mutating: prepared.is_mutating,
    };
    let tls_authenticated = prepared.expected_origin.scheme() == "https";
    let origin_matches = response_url.origin() == prepared.expected_origin.origin();

    if !status.is_success() {
        if (300..400).contains(&status_u16) {
            let kind = error_kind_for_cloud_status_and_code(status_u16, None);
            metadata.set_provenance(EffectProvenance::transport_fault(
                prepared.operation_kind,
                prepared.attempt.as_ref(),
                prepared.resolved_digest.as_deref(),
                metadata
                    .cloud_header("decapod-cloud-request-id")
                    .map(str::to_owned),
                Some("provenance_origin_mismatch"),
                auth_scheme,
                false,
                tls_authenticated,
            ));
            return (
                Err(post_send_error(
                    kind,
                    prepared.is_mutating,
                    "cloud authentication redirect is not permitted",
                )),
                Some(metadata),
            );
        }
        let envelope = check_json_depth(bytes)
            .then(|| serde_json::from_slice::<serde_json::Value>(bytes).ok())
            .flatten();
        let code = envelope
            .as_ref()
            .and_then(public_error_code)
            .or_else(|| dpop_challenge.then_some("use_dpop_nonce"));
        if let Err(prov_err) =
            bind_response_provenance(headers, envelope.as_ref(), &prov_ctx, &mut metadata, code)
        {
            let err = if matches!(
                prov_err.adapter_kind(),
                Some(AdapterErrorKind::IdempotencyConflict)
            ) {
                prov_err
            } else if prepared.is_mutating {
                mark_remote_uncertain(prov_err)
            } else {
                prov_err
            };
            return (Err(err), Some(metadata));
        }
        extract_error_recovery(envelope.as_ref(), origin_matches, &mut metadata);
        let prov_uncertain = metadata
            .provenance()
            .is_some_and(|p| p.status() == EffectStatus::Uncertain);
        let recovery_outcome_unknown = metadata
            .recovery()
            .is_some_and(|r| r.action_category() == RecoveryActionCategory::OutcomeUnknown);
        let recovery_after_send_retry = metadata.recovery().is_some_and(|r| {
            r.execution_phase() == ExecutionPhase::AfterSend
                && r.action_category() == RecoveryActionCategory::RecoverableWithinAuthority
        });
        if prepared.is_mutating
            && (matches!(status_u16, 408 | 500..=599)
                || prov_uncertain
                || recovery_outcome_unknown
                || recovery_after_send_retry)
        {
            let explicit_non_commit = match metadata.provenance().map(|p| p.status()) {
                Some(
                    EffectStatus::RolledBack | EffectStatus::NotAttempted | EffectStatus::Refused,
                ) => true,
                Some(EffectStatus::Uncertain | EffectStatus::CommittedObserved) => false,
                Some(EffectStatus::UnsupportedProvenance) | None => {
                    metadata.recovery().is_some_and(|r| {
                        r.execution_phase() == ExecutionPhase::PreDispatch
                            && r.action_category() != RecoveryActionCategory::OutcomeUnknown
                    })
                }
            };
            if !explicit_non_commit {
                let kind = error_kind_for_cloud_status_and_code(status_u16, code);
                return (
                    Err(post_send_error(
                        kind,
                        true,
                        "cloud authentication request outcome uncertain",
                    )),
                    Some(metadata),
                );
            } else if recovery_outcome_unknown {
                let kind = error_kind_for_cloud_status_and_code(status_u16, code);
                let err = if let Some(code) = code {
                    DactylError::adapter_with_code(kind, code, "cloud request outcome uncertain")
                } else {
                    DactylError::adapter(kind, "cloud request outcome uncertain")
                };
                return (Err(mark_remote_uncertain(err)), Some(metadata));
            }
        }
        let kind = error_kind_for_cloud_status_and_code(status_u16, code);
        let err = if let Some(code) = code {
            DactylError::adapter_with_code(kind, code, "cloud request was rejected")
        } else {
            DactylError::adapter(kind, "cloud authentication request was rejected")
        };
        return (Err(err), Some(metadata));
    }

    if !check_json_depth(bytes) {
        let req_id = origin_matches
            .then(|| {
                metadata
                    .cloud_header("decapod-cloud-request-id")
                    .map(str::to_owned)
            })
            .flatten();
        metadata.set_provenance(EffectProvenance::transport_fault(
            prepared.operation_kind,
            prepared.attempt.as_ref(),
            prepared.resolved_digest.as_deref(),
            req_id,
            Some("payload_too_deep"),
            auth_scheme,
            origin_matches,
            tls_authenticated,
        ));
        return (
            Err(post_send_error(
                AdapterErrorKind::Protocol,
                prepared.is_mutating,
                "cloud response exceeded JSON nesting limit",
            )),
            Some(metadata),
        );
    }

    let value: serde_json::Value =
        match serde_json::from_slice(if bytes.is_empty() { b"null" } else { bytes }) {
            Ok(v) => v,
            Err(_) => {
                let req_id = origin_matches
                    .then(|| {
                        metadata
                            .cloud_header("decapod-cloud-request-id")
                            .map(str::to_owned)
                    })
                    .flatten();
                metadata.set_provenance(EffectProvenance::transport_fault(
                    prepared.operation_kind,
                    prepared.attempt.as_ref(),
                    prepared.resolved_digest.as_deref(),
                    req_id,
                    Some("malformed_response"),
                    auth_scheme,
                    origin_matches,
                    tls_authenticated,
                ));
                return (
                    Err(post_send_error(
                        AdapterErrorKind::Protocol,
                        prepared.is_mutating,
                        "invalid cloud authentication response",
                    )),
                    Some(metadata),
                );
            }
        };

    if let Err(prov_err) =
        bind_response_provenance(headers, Some(&value), &prov_ctx, &mut metadata, None)
    {
        let err = if matches!(
            prov_err.adapter_kind(),
            Some(
                AdapterErrorKind::Authorization
                    | AdapterErrorKind::TransactionAborted
                    | AdapterErrorKind::IdempotencyConflict
            )
        ) {
            prov_err
        } else if prepared.is_mutating {
            mark_remote_uncertain(prov_err)
        } else {
            prov_err
        };
        return (Err(err), Some(metadata));
    }

    let parsed = serde_json::from_value(value).map_err(|_| {
        post_send_error(
            AdapterErrorKind::Protocol,
            prepared.is_mutating,
            "invalid cloud authentication response",
        )
    });
    if parsed.is_err() && prepared.is_mutating {
        let req_id = origin_matches
            .then(|| {
                metadata
                    .cloud_header("decapod-cloud-request-id")
                    .map(str::to_owned)
            })
            .flatten();
        metadata.set_provenance(EffectProvenance::transport_fault(
            prepared.operation_kind,
            prepared.attempt.as_ref(),
            prepared.resolved_digest.as_deref(),
            req_id,
            Some("malformed_response"),
            auth_scheme,
            origin_matches,
            tls_authenticated,
        ));
    }
    (parsed, Some(metadata))
}

fn fault_metadata(
    prepared: &PreparedCloudRequest,
    auth: &AuthHeaders,
    mut metadata: CloudResponseMetadata,
    reason_code: &'static str,
    origin_bound: bool,
) -> CloudResponseMetadata {
    let req_id = origin_bound
        .then(|| {
            metadata
                .cloud_header("decapod-cloud-request-id")
                .map(str::to_owned)
        })
        .flatten();
    metadata.set_provenance(EffectProvenance::transport_fault(
        prepared.operation_kind,
        prepared.attempt.as_ref(),
        prepared.resolved_digest.as_deref(),
        req_id,
        Some(reason_code),
        effective_auth_scheme(auth),
        origin_bound,
        prepared.expected_origin.scheme() == "https",
    ));
    metadata
}

fn pre_send_timeout_outcome<T>(
    prepared: &PreparedCloudRequest,
    auth: &AuthHeaders,
) -> HttpOutcome<T> {
    let mut meta = CloudResponseMetadata::default();
    meta.set_provenance(EffectProvenance::local_preflight(
        prepared.operation_kind,
        prepared.attempt.as_ref(),
        Some("timeout"),
        effective_auth_scheme(auth),
    ));
    (
        Err(DactylError::adapter(
            AdapterErrorKind::Timeout,
            "local preflight: request deadline expired before dispatch",
        )),
        Some(meta),
    )
}

fn send_auth_once<T: for<'de> Deserialize<'de>>(
    blocking_client: &reqwest::blocking::Client,
    prepared: &PreparedCloudRequest,
    auth: &AuthHeaders,
    deadline: Instant,
    proof_signer: Option<&Arc<dyn RequestProofSigner>>,
    unauthorized: &mut bool,
) -> HttpOutcome<T> {
    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        return pre_send_timeout_outcome(prepared, auth);
    }
    let mut request = blocking_client
        .request(prepared.method.clone(), &prepared.endpoint)
        .timeout(remaining)
        .header("Accept", "application/json");
    if let Some(key) = prepared.key.as_ref() {
        request = request.header("apikey", key.clone());
    }
    if let Some(auth_header) = auth.0.as_ref() {
        request = request.header(reqwest::header::AUTHORIZATION, auth_header.clone());
    }
    if let Some(dpop) = auth.1.as_ref() {
        request = request.header("DPoP", dpop.clone());
    }
    if let Some((provider, host, token)) = prepared.scm.as_ref() {
        request = request
            .header("X-SCM-Provider", *provider)
            .header("X-SCM-Host", *host)
            .header("X-SCM-Token", token.clone());
    }
    for (name, value) in &prepared.attempt_headers {
        request = request.header(*name, value.clone());
    }
    if let Some(body) = prepared.body.as_deref() {
        request = request
            .header("Content-Type", "application/json")
            .body(body.to_vec());
    }
    let response = match request.send() {
        Ok(response) => response,
        Err(e) => {
            let meta = fault_metadata(
                prepared,
                auth,
                CloudResponseMetadata::default(),
                "outcome_unknown",
                false,
            );
            return (
                Err(if e.is_timeout() {
                    post_send_error(
                        AdapterErrorKind::Timeout,
                        prepared.is_mutating,
                        "cloud authentication timed out",
                    )
                } else {
                    post_send_error(
                        AdapterErrorKind::Unavailable,
                        prepared.is_mutating,
                        "cloud authentication transport failed",
                    )
                }),
                Some(meta),
            );
        }
    };
    let status = response.status();
    let response_url = response.url().clone();
    let response_headers = response.headers().clone();
    *unauthorized = status == reqwest::StatusCode::UNAUTHORIZED;
    let metadata = extract_response_metadata(&response_headers, &prepared.canonical, proof_signer);
    let origin_matches = response_url.origin() == prepared.expected_origin.origin();
    let mut bytes = Vec::new();
    if let Err(error) = response.take(BODY_LIMIT + 1).read_to_end(&mut bytes) {
        let meta = fault_metadata(prepared, auth, metadata, "outcome_unknown", origin_matches);
        return (
            Err(if crate::adapter::supabase::body_read_timed_out(&error) {
                post_send_error(
                    AdapterErrorKind::Timeout,
                    prepared.is_mutating,
                    "cloud authentication timed out",
                )
            } else {
                post_send_error(
                    AdapterErrorKind::Unavailable,
                    prepared.is_mutating,
                    "cloud authentication transport failed",
                )
            }),
            Some(meta),
        );
    }
    if bytes.len() as u64 > BODY_LIMIT {
        let meta = fault_metadata(
            prepared,
            auth,
            metadata,
            "response_too_large",
            origin_matches,
        );
        return (
            Err(post_send_error(
                AdapterErrorKind::Protocol,
                prepared.is_mutating,
                "auth response exceeds body limit",
            )),
            Some(meta),
        );
    }
    handle_cloud_response(
        prepared,
        auth,
        status,
        &response_headers,
        &response_url,
        &bytes,
        metadata,
    )
}

async fn send_auth_once_async<T: for<'de> Deserialize<'de>>(
    async_client: &reqwest::Client,
    prepared: &PreparedCloudRequest,
    auth: &AuthHeaders,
    deadline: Instant,
    proof_signer: Option<&Arc<dyn RequestProofSigner>>,
) -> (HttpOutcome<T>, bool) {
    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        return (pre_send_timeout_outcome(prepared, auth), false);
    }
    let mut request = async_client
        .request(prepared.method.clone(), &prepared.endpoint)
        .timeout(remaining)
        .header("Accept", "application/json");
    if let Some(key) = prepared.key.as_ref() {
        request = request.header("apikey", key.clone());
    }
    if let Some(auth_header) = auth.0.as_ref() {
        request = request.header(reqwest::header::AUTHORIZATION, auth_header.clone());
    }
    if let Some(dpop) = auth.1.as_ref() {
        request = request.header("DPoP", dpop.clone());
    }
    if let Some((provider, host, token)) = prepared.scm.as_ref() {
        request = request
            .header("X-SCM-Provider", *provider)
            .header("X-SCM-Host", *host)
            .header("X-SCM-Token", token.clone());
    }
    for (name, value) in &prepared.attempt_headers {
        request = request.header(*name, value.clone());
    }
    if let Some(body) = prepared.body.as_deref() {
        request = request
            .header("Content-Type", "application/json")
            .body(body.to_vec());
    }
    let response = match tokio::time::timeout(remaining, request.send()).await {
        Ok(Ok(response)) => response,
        Ok(Err(e)) => {
            let meta = fault_metadata(
                prepared,
                auth,
                CloudResponseMetadata::default(),
                "outcome_unknown",
                false,
            );
            return (
                (
                    Err(if e.is_timeout() {
                        post_send_error(
                            AdapterErrorKind::Timeout,
                            prepared.is_mutating,
                            "cloud authentication timed out",
                        )
                    } else {
                        post_send_error(
                            AdapterErrorKind::Unavailable,
                            prepared.is_mutating,
                            "cloud authentication transport failed",
                        )
                    }),
                    Some(meta),
                ),
                false,
            );
        }
        Err(_) => {
            let meta = fault_metadata(
                prepared,
                auth,
                CloudResponseMetadata::default(),
                "outcome_unknown",
                false,
            );
            return (
                (
                    Err(post_send_error(
                        AdapterErrorKind::Timeout,
                        prepared.is_mutating,
                        "cloud authentication timed out",
                    )),
                    Some(meta),
                ),
                false,
            );
        }
    };
    let status = response.status();
    let unauthorized = status == reqwest::StatusCode::UNAUTHORIZED;
    let response_url = response.url().clone();
    let response_headers = response.headers().clone();
    let metadata = extract_response_metadata(&response_headers, &prepared.canonical, proof_signer);
    let origin_matches = response_url.origin() == prepared.expected_origin.origin();
    let body_remaining = deadline.saturating_duration_since(Instant::now());
    let bytes = match tokio::time::timeout(
        body_remaining,
        read_bounded_body_async(response, BODY_LIMIT),
    )
    .await
    {
        Ok(Ok(bytes)) => bytes,
        Ok(Err(AsyncBodyError::TooLarge)) => {
            let meta = fault_metadata(
                prepared,
                auth,
                metadata,
                "response_too_large",
                origin_matches,
            );
            return (
                (
                    Err(post_send_error(
                        AdapterErrorKind::Protocol,
                        prepared.is_mutating,
                        "auth response exceeds body limit",
                    )),
                    Some(meta),
                ),
                unauthorized,
            );
        }
        Ok(Err(AsyncBodyError::Transport(e))) => {
            let meta = fault_metadata(prepared, auth, metadata, "outcome_unknown", origin_matches);
            return (
                (
                    Err(if e.is_timeout() {
                        post_send_error(
                            AdapterErrorKind::Timeout,
                            prepared.is_mutating,
                            "cloud authentication timed out",
                        )
                    } else {
                        post_send_error(
                            AdapterErrorKind::Unavailable,
                            prepared.is_mutating,
                            "cloud authentication transport failed",
                        )
                    }),
                    Some(meta),
                ),
                unauthorized,
            );
        }
        Err(_) => {
            let meta = fault_metadata(prepared, auth, metadata, "outcome_unknown", origin_matches);
            return (
                (
                    Err(post_send_error(
                        AdapterErrorKind::Timeout,
                        prepared.is_mutating,
                        "cloud authentication timed out",
                    )),
                    Some(meta),
                ),
                unauthorized,
            );
        }
    };
    (
        handle_cloud_response(
            prepared,
            auth,
            status,
            &response_headers,
            &response_url,
            &bytes,
            metadata,
        ),
        unauthorized,
    )
}

#[allow(clippy::too_many_arguments)]
fn request<T: for<'de> Deserialize<'de> + Send + 'static>(
    http_client: &Arc<SharedHttpClient>,
    http_budget: ResourceBudget,
    origin: &str,
    key: Option<&str>,
    method: &str,
    path: &str,
    body: Option<serde_json::Value>,
    bearer: Option<&str>,
    scm: Option<&ScmCredential>,
    auth_policy: RequestAuthPolicy<'_>,
    dpop_nonce: Option<&str>,
    attempt: Option<&OperationAttempt>,
    operation_kind: Option<OperationKind>,
    observation_source: ObservationSource,
    is_mutating: bool,
    metadata_out: &mut CloudResponseMetadata,
) -> Result<T, DactylError> {
    let mut prepared = prepare_cloud_request(
        origin,
        key,
        method,
        path,
        body,
        bearer,
        scm,
        auth_policy,
        dpop_nonce,
        attempt,
        operation_kind,
        observation_source,
        is_mutating,
        http_budget,
        metadata_out,
    )?;
    let http_client = http_client.clone();
    let proof_signer = auth_policy.proof_signer.cloned();
    let managed_bearer_proof = auth_policy.managed_bearer_proof;

    let mut execute = move || -> HttpOutcome<T> {
        let blocking_client = match http_client.blocking_client() {
            Ok(c) => c,
            Err(err) => {
                let auth = (prepared.auth_header.take(), prepared.dpop_header.take());
                let mut meta = CloudResponseMetadata::default();
                meta.set_provenance(EffectProvenance::local_preflight(
                    prepared.operation_kind,
                    prepared.attempt.as_ref(),
                    err.adapter_code(),
                    effective_auth_scheme(&auth),
                ));
                return (Err(err), Some(meta));
            }
        };
        let deadline = Instant::now() + prepared.request_timeout;
        let _permit = match http_client.acquire_permit(deadline) {
            Ok(permit) => permit,
            Err(error) => {
                let auth = (prepared.auth_header.take(), prepared.dpop_header.take());
                let mut meta = CloudResponseMetadata::default();
                meta.set_provenance(EffectProvenance::local_preflight(
                    prepared.operation_kind,
                    prepared.attempt.as_ref(),
                    error.adapter_code(),
                    effective_auth_scheme(&auth),
                ));
                return (Err(error), Some(meta));
            }
        };
        let auth = (prepared.auth_header.take(), prepared.dpop_header.take());
        let (outcome, _) = with_dpop_nonce_retry(
            &prepared.method_str,
            &prepared.canonical,
            proof_signer.as_ref(),
            auth,
            managed_bearer_proof,
            |auth, unauthorized| {
                send_auth_once(
                    &blocking_client,
                    &prepared,
                    auth,
                    deadline,
                    proof_signer.as_ref(),
                    unauthorized,
                )
            },
        );
        outcome
    };

    let (outcome, captured_meta) = if in_tokio_runtime() {
        std::thread::Builder::new()
            .name("dactyl-cloud-auth".into())
            .spawn(execute)
            .map_err(|_| {
                DactylError::adapter(
                    AdapterErrorKind::Unavailable,
                    "local preflight: auth worker unavailable",
                )
            })?
            .join()
            .map_err(|_| {
                post_send_error(
                    AdapterErrorKind::Unavailable,
                    is_mutating,
                    "auth worker failed",
                )
            })?
    } else {
        execute()
    };
    *metadata_out = captured_meta.unwrap_or_default();
    outcome
}

#[allow(clippy::too_many_arguments)]
async fn request_async<T: for<'de> Deserialize<'de> + Send + 'static>(
    http_client: &Arc<SharedHttpClient>,
    http_budget: ResourceBudget,
    origin: &str,
    key: Option<&str>,
    method: &str,
    path: &str,
    body: Option<serde_json::Value>,
    bearer: Option<&str>,
    scm: Option<&ScmCredential>,
    auth_policy: RequestAuthPolicy<'_>,
    dpop_nonce: Option<&str>,
    attempt: Option<&OperationAttempt>,
    operation_kind: Option<OperationKind>,
    observation_source: ObservationSource,
    is_mutating: bool,
    metadata_out: &mut CloudResponseMetadata,
) -> Result<T, DactylError> {
    let mut prepared = prepare_cloud_request(
        origin,
        key,
        method,
        path,
        body,
        bearer,
        scm,
        auth_policy,
        dpop_nonce,
        attempt,
        operation_kind,
        observation_source,
        is_mutating,
        http_budget,
        metadata_out,
    )?;
    let async_client = http_client.async_client()?;
    let proof_signer = auth_policy.proof_signer.cloned();
    let managed_bearer_proof = auth_policy.managed_bearer_proof;
    let deadline = Instant::now() + prepared.request_timeout;
    let auth = (prepared.auth_header.take(), prepared.dpop_header.take());
    let _permit = match http_client.acquire_permit_async(deadline).await {
        Ok(permit) => permit,
        Err(error) => {
            *metadata_out = CloudResponseMetadata::default();
            metadata_out.set_provenance(EffectProvenance::local_preflight(
                prepared.operation_kind,
                prepared.attempt.as_ref(),
                error.adapter_code(),
                effective_auth_scheme(&auth),
            ));
            return Err(error);
        }
    };
    let method_str = prepared.method_str.clone();
    let canonical = prepared.canonical.clone();
    let (outcome, _) = with_dpop_nonce_retry_async(
        &method_str,
        &canonical,
        proof_signer.as_ref(),
        auth,
        managed_bearer_proof,
        |auth| {
            let client = &async_client;
            let req_ref = &prepared;
            let signer_ref = proof_signer.as_ref();
            async move { send_auth_once_async(client, req_ref, &auth, deadline, signer_ref).await }
        },
    )
    .await;
    let (res, captured_meta) = outcome;
    *metadata_out = captured_meta.unwrap_or_default();
    res
}
