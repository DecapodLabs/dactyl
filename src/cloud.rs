//! Logical Decapod Cloud connector, public deployment discovery and stateless auth I/O.
//! The application owns browser interaction, key custody, and private session persistence.

use std::io::Read;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde::Deserialize;

use crate::budget::mark_remote_uncertain;
use crate::connector::{
    validate_dpop_nonce, validate_opaque_credential, AccountOperation, AuthenticationSecrets,
    AuthenticationStart, CloudResponseMetadata, RequestProofSigner, ScmCredential, UserSession,
    DEFAULT_CLOUD_ORIGIN,
};
use crate::http_pool::{
    acquire_client, acquire_permit, build_auth_headers, canonical_url, extract_response_metadata,
    SharedHttpClient,
};
use crate::{
    AdapterErrorKind, DactylError, DatastoreRoute, OperationKind, ResourceBudget, SupabaseRpcConfig,
};

const CANONICAL_ONBOARDING: &str = "/functions/v1/decapod-onboarding";
const LEGACY_ONBOARDING: &str = "/functions/v1/propodus-onboarding";
const BODY_LIMIT: u64 = 16 * 1024;
const MAX_JSON_DEPTH: usize = 64;
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(15);

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

    /// Discover the version-1 Decapod Cloud deployment profile, returning bounded
    /// diagnostic/sunset response headers (`Decapod-Cloud-*`, `Sunset`, etc.).
    ///
    /// Probes `/functions/v1/decapod-onboarding/config` first and falls back once
    /// to `/functions/v1/propodus-onboarding/config` only on HTTP `404`.
    pub fn discover_with_metadata(
        origin: &str,
    ) -> Result<(Self, CloudResponseMetadata), DactylError> {
        validate_origin(origin)?;
        let http_client = acquire_client(CONNECT_TIMEOUT)?;
        let mut metadata = CloudResponseMetadata::default();
        let mut onboarding_prefix = OnboardingPrefix::Canonical;
        let profile: DeploymentProfile = match request(
            &http_client,
            origin,
            None,
            "GET",
            &format!("{CANONICAL_ONBOARDING}/config"),
            None,
            None,
            None,
            None,
            None,
            false,
            &mut metadata,
        ) {
            Ok(profile) => profile,
            Err(error) if is_missing_onboarding_function(&error) => {
                onboarding_prefix = OnboardingPrefix::Legacy;
                metadata = CloudResponseMetadata::default();
                request(
                    &http_client,
                    origin,
                    None,
                    "GET",
                    &format!("{LEGACY_ONBOARDING}/config"),
                    None,
                    None,
                    None,
                    None,
                    None,
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
                http_client,
            },
            metadata,
        ))
    }

    /// Explicit seam for a trusted deployment profile or a disposable fixture.
    pub fn with_config(config: SupabaseRpcConfig) -> Result<Self, DactylError> {
        crate::adapter::supabase::validate_rpc(&config)?;
        let http_client = acquire_client(CONNECT_TIMEOUT)?;
        Ok(Self {
            config,
            onboarding_prefix: OnboardingPrefix::Canonical,
            proof_signer: None,
            dpop_nonce: None,
            http_client,
        })
    }

    /// Attach a synchronous caller-supplied [`RequestProofSigner`] for Decapod
    /// Cloud account, control RPC, and datastore requests.
    pub fn with_proof_signer(mut self, signer: Arc<dyn RequestProofSigner>) -> Self {
        self.proof_signer = Some(signer);
        self
    }

    /// Select the Cloud Edge data gateway for an already issued device session.
    pub fn with_device_gateway(mut self) -> Self {
        self.config = self.config.with_device_gateway();
        self
    }

    /// Supply an initial server `DPoP-Nonce` challenge value for this connector.
    pub fn with_dpop_nonce(mut self, nonce: impl Into<String>) -> Self {
        self.dpop_nonce = Some(nonce.into());
        self
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

    pub fn origin(&self) -> &str {
        self.config.project_origin()
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
        crate::adapter::supabase::control_request_with_auth(
            &self.config,
            function,
            request,
            bearer,
            dpop_nonce.or(self.dpop_nonce.as_deref()),
            kind,
            budget,
            self.proof_signer.clone(),
            Some(self.http_client.clone()),
            metadata_out,
        )
    }

    /// Transport an opaque versioned account, device-enrollment, or billing
    /// request using the current user bearer. Optional SCM credentials are for
    /// explicit account linking only, never data or Auth token requests. Dactyl
    /// does not infer identity, roles, pricing or consent, including from a
    /// managed Auth provider token.
    ///
    /// Mutating operations (`Register`, `Checkout`, `Manage`, `Portal`,
    /// `DeviceEnroll`, `DeviceStepUp`, `DeviceRevoke`) attach error code
    /// `"outcome_unknown"` and [`crate::MutationOutcome::Uncertain`] if
    /// transport fails after dispatch so callers reconcile server state rather
    /// than blindly replaying a write.
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
        self.onboarding_request(
            operation.action(),
            Some(body.clone()),
            Some(bearer),
            scm,
            dpop_nonce,
            operation.is_mutating(),
            metadata_out,
        )
    }

    pub fn start_authentication(
        &self,
        repository: &str,
        secrets: &AuthenticationSecrets,
    ) -> Result<AuthenticationStart, DactylError> {
        secrets.validate()?;
        let response: AuthenticationStart = self.auth_request(
            "start",
            &serde_json::json!({
                "version": 1,
                "repository": repository,
                "pkce_challenge": AuthenticationSecrets::challenge(&secrets.verifier),
                "poll_challenge": AuthenticationSecrets::challenge(&secrets.poll_secret)
            }),
            None,
            false,
        )?;
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
        match response.get("status").and_then(|v| v.as_str()) {
            Some(status @ ("pending" | "ready" | "consumed" | "complete")) => Ok(status.into()),
            _ => Err(protocol("invalid authentication status")),
        }
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

    pub fn refresh_session(&self, refresh_token: &str) -> Result<UserSession, DactylError> {
        opaque(refresh_token)?;
        self.token_request(
            "refresh_token",
            serde_json::json!({"refresh_token": refresh_token}),
        )
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
        if result.get("status").and_then(|v| v.as_str()) != Some("complete") {
            return Err(DactylError::adapter_with_code(
                AdapterErrorKind::Protocol,
                "outcome_unknown",
                "authentication completion was not confirmed",
            ));
        }
        Ok(())
    }

    pub fn revoke_session(&self, bearer: &str) -> Result<(), DactylError> {
        let mut metadata = CloudResponseMetadata::default();
        let _: serde_json::Value = request(
            &self.http_client,
            self.origin(),
            Some(self.config.publishable_key()),
            "POST",
            "/auth/v1/logout?scope=local",
            None,
            Some(bearer),
            None,
            self.proof_signer.as_ref(),
            self.dpop_nonce.as_deref(),
            true,
            &mut metadata,
        )?;
        Ok(())
    }

    fn token_request(
        &self,
        grant: &str,
        body: serde_json::Value,
    ) -> Result<UserSession, DactylError> {
        #[derive(Deserialize)]
        struct Response {
            access_token: String,
            refresh_token: String,
            expires_in: u64,
            #[serde(default)]
            provider_token: Option<String>,
        }
        let mut metadata = CloudResponseMetadata::default();
        let response: Response = request(
            &self.http_client,
            self.origin(),
            Some(self.config.publishable_key()),
            "POST",
            &format!("/auth/v1/token?grant_type={grant}"),
            Some(body),
            None,
            None,
            None,
            None,
            true,
            &mut metadata,
        )?;
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

    fn auth_request<T: for<'de> Deserialize<'de> + Send + 'static>(
        &self,
        action: &str,
        body: &serde_json::Value,
        bearer: Option<&str>,
        is_mutating: bool,
    ) -> Result<T, DactylError> {
        let mut metadata = CloudResponseMetadata::default();
        self.onboarding_request(
            action,
            Some(body.clone()),
            bearer,
            None,
            None,
            is_mutating,
            &mut metadata,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn onboarding_request<T: for<'de> Deserialize<'de> + Send + 'static>(
        &self,
        action: &str,
        body: Option<serde_json::Value>,
        bearer: Option<&str>,
        scm: Option<&ScmCredential>,
        dpop_nonce: Option<&str>,
        is_mutating: bool,
        metadata_out: &mut CloudResponseMetadata,
    ) -> Result<T, DactylError> {
        let primary_prefix = self.onboarding_prefix.as_str();
        let nonce = dpop_nonce.or(self.dpop_nonce.as_deref());
        match request(
            &self.http_client,
            self.origin(),
            Some(self.config.publishable_key()),
            "POST",
            &format!("{primary_prefix}/{action}"),
            body.clone(),
            bearer,
            scm,
            self.proof_signer.as_ref(),
            nonce,
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
                    self.origin(),
                    Some(self.config.publishable_key()),
                    "POST",
                    &format!("{LEGACY_ONBOARDING}/{action}"),
                    body,
                    bearer,
                    scm,
                    self.proof_signer.as_ref(),
                    nonce,
                    is_mutating,
                    metadata_out,
                )
            }
            Err(error) => Err(error),
        }
    }
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

    match raw {
        "authentication_required" => Some("authentication_required"),
        "invalid_token" => Some("invalid_token"),
        "invalid_grant" => Some("invalid_grant"),
        "bad_jwt" => Some("bad_jwt"),
        "session_expired" => Some("session_expired"),
        "session_revoked" => Some("session_revoked"),
        "session_not_found" => Some("session_not_found"),
        "refresh_token_not_found" => Some("refresh_token_not_found"),
        "refresh_token_already_used" => Some("refresh_token_already_used"),
        "use_dpop_nonce" => Some("use_dpop_nonce"),
        "invalid_dpop_proof" => Some("invalid_dpop_proof"),
        "dpop_proof_required" => Some("dpop_proof_required"),
        "missing_dpop_proof" => Some("missing_dpop_proof"),
        "dpop_replay" => Some("dpop_replay"),
        "dpop_nonce_mismatch" => Some("dpop_nonce_mismatch"),
        "dpop_clock_skew" => Some("dpop_clock_skew"),
        "dpop_ath_mismatch" => Some("dpop_ath_mismatch"),
        "dpop_htu_mismatch" => Some("dpop_htu_mismatch"),
        "dpop_htm_mismatch" => Some("dpop_htm_mismatch"),
        "dpop_jkt_mismatch" => Some("dpop_jkt_mismatch"),
        "stolen_bearer" => Some("stolen_bearer"),
        "device_revoked" => Some("device_revoked"),
        "device_not_enrolled" => Some("device_not_enrolled"),
        "authorization_denied" => Some("authorization_denied"),
        "repository_not_authorized" => Some("repository_not_authorized"),
        "entitlement_required" => Some("entitlement_required"),
        "organization_seat_required" => Some("organization_seat_required"),
        "seat_required" => Some("seat_required"),
        "seat_unassigned" => Some("seat_unassigned"),
        "seat_revoked" => Some("seat_revoked"),
        "account_setup_required" => Some("account_setup_required"),
        "identity_link_required" => Some("identity_link_required"),
        "custody_conflict" => Some("custody_conflict"),
        "owner_protected" => Some("owner_protected"),
        "member_not_found" => Some("member_not_found"),
        "invitation_expired" => Some("invitation_expired"),
        "invitation_not_found" => Some("invitation_not_found"),
        "project_not_found" => Some("project_not_found"),
        "quote_confirmation_required" => Some("quote_confirmation_required"),
        "billing_binding_required" => Some("billing_binding_required"),
        "payment_pending" => Some("payment_pending"),
        "billing_not_configured" => Some("billing_not_configured"),
        "billing_unavailable" => Some("billing_unavailable"),
        "provider_unavailable" => Some("provider_unavailable"),
        "invalid_request" => Some("invalid_request"),
        "payload_too_large" => Some("payload_too_large"),
        "rate_limited" => Some("rate_limited"),
        "method_not_allowed" => Some("method_not_allowed"),
        "not_found" => Some("not_found"),
        "flow_expired" => Some("flow_expired"),
        "flow_not_found" => Some("flow_not_found"),
        "invalid_flow_state" => Some("invalid_flow_state"),
        _ => None,
    }
}

#[allow(clippy::too_many_arguments)]
fn request<T: for<'de> Deserialize<'de> + Send + 'static>(
    http_client: &Arc<SharedHttpClient>,
    origin: &str,
    key: Option<&str>,
    method: &str,
    path: &str,
    body: Option<serde_json::Value>,
    bearer: Option<&str>,
    scm: Option<&ScmCredential>,
    proof_signer: Option<&Arc<dyn RequestProofSigner>>,
    dpop_nonce: Option<&str>,
    is_mutating: bool,
    metadata_out: &mut CloudResponseMetadata,
) -> Result<T, DactylError> {
    validate_origin(origin)?;
    let endpoint = format!("{}{path}", origin.trim_end_matches('/'));
    let canonical = canonical_url(&endpoint)?;
    let method_str = method.to_owned();
    let method = reqwest::Method::from_bytes(method.as_bytes())
        .map_err(|_| protocol("invalid auth request method"))?;
    let header = |value: &str| -> Result<reqwest::header::HeaderValue, DactylError> {
        let mut header = reqwest::header::HeaderValue::from_str(value)
            .map_err(|_| protocol("invalid authentication header"))?;
        header.set_sensitive(true);
        Ok(header)
    };
    let key = key.map(header).transpose()?;
    if let Some(nonce) = dpop_nonce {
        validate_dpop_nonce(nonce)?;
    }
    let (auth_header, dpop_header) = match bearer {
        Some(value) => {
            opaque(value)?;
            let (auth, dpop) =
                build_auth_headers(&method_str, &canonical, value, proof_signer, dpop_nonce)?;
            (Some(auth), dpop)
        }
        None => (None, None),
    };
    let scm = scm
        .map(|credential| {
            Ok::<_, DactylError>((
                credential.provider().label(),
                credential.host(),
                header(credential.token())?,
            ))
        })
        .transpose()?;
    let body = body
        .map(|v| serde_json::to_vec(&v).map_err(|_| protocol("invalid auth request")))
        .transpose()?;
    if body.as_ref().is_some_and(|v| v.len() as u64 > BODY_LIMIT) {
        return Err(protocol("auth request exceeds body limit"));
    }
    let http_client = http_client.clone();
    let proof_signer = proof_signer.cloned();
    let (outcome, captured_meta) = std::thread::Builder::new()
        .name("dactyl-cloud-auth".into())
        .spawn(
            move || -> (Result<T, DactylError>, Option<CloudResponseMetadata>) {
                let deadline = Instant::now() + REQUEST_TIMEOUT;
                let _permit = match acquire_permit(deadline) {
                    Ok(permit) => permit,
                    Err(error) => return (Err(error), None),
                };
                let remaining = deadline.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    return (
                        Err(DactylError::adapter(
                            AdapterErrorKind::Timeout,
                            "cloud authentication timed out",
                        )),
                        None,
                    );
                }
                let mut request = http_client
                    .client()
                    .request(method, endpoint)
                    .timeout(remaining)
                    .header("Accept", "application/json");
                if let Some(key) = key {
                    request = request.header("apikey", key);
                }
                if let Some(auth) = auth_header {
                    request = request.header(reqwest::header::AUTHORIZATION, auth);
                }
                if let Some(dpop) = dpop_header {
                    request = request.header("DPoP", dpop);
                }
                if let Some((provider, host, token)) = scm {
                    request = request
                        .header("X-SCM-Provider", provider)
                        .header("X-SCM-Host", host)
                        .header("X-SCM-Token", token);
                }
                if let Some(body) = body {
                    request = request
                        .header("Content-Type", "application/json")
                        .body(body);
                }
                let response = match request.send() {
                    Ok(response) => response,
                    Err(e) => {
                        return (
                            Err(if e.is_timeout() {
                                post_send_error(
                                    AdapterErrorKind::Timeout,
                                    is_mutating,
                                    "cloud authentication timed out",
                                )
                            } else {
                                post_send_error(
                                    AdapterErrorKind::Unavailable,
                                    is_mutating,
                                    "cloud authentication transport failed",
                                )
                            }),
                            None,
                        );
                    }
                };
                let status = response.status();
                let metadata = extract_response_metadata(
                    response.headers(),
                    &canonical,
                    proof_signer.as_ref(),
                );
                let mut bytes = Vec::new();
                if let Err(error) = response.take(BODY_LIMIT + 1).read_to_end(&mut bytes) {
                    return (
                        Err(if crate::adapter::supabase::body_read_timed_out(&error) {
                            post_send_error(
                                AdapterErrorKind::Timeout,
                                is_mutating,
                                "cloud authentication timed out",
                            )
                        } else {
                            post_send_error(
                                AdapterErrorKind::Unavailable,
                                is_mutating,
                                "cloud authentication transport failed",
                            )
                        }),
                        Some(metadata),
                    );
                }
                if bytes.len() as u64 > BODY_LIMIT {
                    return (
                        Err(post_send_error(
                            AdapterErrorKind::Protocol,
                            is_mutating,
                            "auth response exceeds body limit",
                        )),
                        Some(metadata),
                    );
                }
                if !status.is_success() {
                    let kind = match status.as_u16() {
                        400 | 401 => AdapterErrorKind::Authentication,
                        403 => AdapterErrorKind::Authorization,
                        404 => AdapterErrorKind::NotFound,
                        409 => AdapterErrorKind::Conflict,
                        413 => AdapterErrorKind::Quota,
                        429 => AdapterErrorKind::RateLimited,
                        500..=599 => AdapterErrorKind::Unavailable,
                        _ => AdapterErrorKind::Protocol,
                    };
                    if is_mutating && matches!(status.as_u16(), 300..=399 | 408 | 500..=599) {
                        return (
                            Err(post_send_error(
                                kind,
                                true,
                                "cloud authentication request outcome uncertain",
                            )),
                            Some(metadata),
                        );
                    }
                    let envelope = check_json_depth(&bytes)
                        .then(|| serde_json::from_slice::<serde_json::Value>(&bytes).ok())
                        .flatten();
                    if let Some(code) = envelope.as_ref().and_then(public_error_code) {
                        return (
                            Err(DactylError::adapter_with_code(
                                kind,
                                code,
                                "cloud request was rejected",
                            )),
                            Some(metadata),
                        );
                    }
                    return (
                        Err(DactylError::adapter(
                            kind,
                            "cloud authentication request was rejected",
                        )),
                        Some(metadata),
                    );
                }
                if !check_json_depth(&bytes) {
                    return (
                        Err(post_send_error(
                            AdapterErrorKind::Protocol,
                            is_mutating,
                            "cloud response exceeded JSON nesting limit",
                        )),
                        Some(metadata),
                    );
                }
                let parsed =
                    serde_json::from_slice(if bytes.is_empty() { b"null" } else { &bytes })
                        .map_err(|_| {
                            post_send_error(
                                AdapterErrorKind::Protocol,
                                is_mutating,
                                "invalid cloud authentication response",
                            )
                        });
                (parsed, Some(metadata))
            },
        )
        .map_err(|_| protocol("auth worker unavailable"))?
        .join()
        .map_err(|_| protocol("auth worker failed"))?;
    if let Some(captured) = captured_meta {
        *metadata_out = captured;
    }
    outcome
}
