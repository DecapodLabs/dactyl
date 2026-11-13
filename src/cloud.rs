//! Logical cloud connector, public deployment discovery and stateless auth I/O.
//! The application owns browser interaction and private session persistence.

use std::io::Read;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::Deserialize;

use crate::connector::{
    validate_opaque_credential, AccountOperation, AuthenticationSecrets, AuthenticationStart,
    ScmCredential, UserSession, DEFAULT_CLOUD_ORIGIN,
};
use crate::{AdapterErrorKind, DactylError, DatastoreRoute, SupabaseRpcConfig};

const ONBOARDING: &str = "/functions/v1/propodus-onboarding";
const BODY_LIMIT: u64 = 16 * 1024;
const MAX_JSON_DEPTH: usize = 64;

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
}

impl std::fmt::Debug for CloudConnector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CloudConnector")
            .field("config", &self.config)
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
        validate_origin(origin)?;
        let profile: DeploymentProfile = request(
            origin,
            None,
            "GET",
            &format!("{ONBOARDING}/config"),
            None,
            None,
            None,
            false,
        )?;
        if profile.version != 1
            || profile.project_origin.trim_end_matches('/') != origin.trim_end_matches('/')
        {
            return Err(protocol(
                "cloud deployment profile version or origin mismatch",
            ));
        }
        Self::with_config(
            SupabaseRpcConfig::v1(profile.project_origin, profile.publishable_key)
                .with_schema(profile.schema),
        )
    }

    /// Explicit seam for a trusted deployment profile or a disposable fixture.
    pub fn with_config(config: SupabaseRpcConfig) -> Result<Self, DactylError> {
        crate::adapter::supabase::validate_rpc(&config)?;
        Ok(Self { config })
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
        DatastoreRoute::supabase_rpc(self.config.clone(), bearer)
    }

    /// Transport an opaque versioned account request using the current user
    /// bearer. Optional SCM credentials are for explicit account linking only,
    /// never data or Auth token requests. Dactyl does not infer identity, roles,
    /// pricing or consent, including from a managed Auth provider token.
    ///
    /// Mutating operations (`Register`, `Checkout`, `Manage`, `Portal`) attach
    /// error code `"outcome_unknown"` if transport fails after dispatch so
    /// callers reconcile server state rather than blindly replaying a write.
    pub fn account_request(
        &self,
        operation: AccountOperation,
        body: &serde_json::Value,
        bearer: &str,
        scm: Option<&ScmCredential>,
    ) -> Result<serde_json::Value, DactylError> {
        request(
            self.origin(),
            Some(self.config.publishable_key()),
            "POST",
            &format!("{ONBOARDING}/{}", operation.action()),
            Some(body.clone()),
            Some(bearer),
            scm,
            operation.is_mutating(),
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
            || browser.path() != format!("{ONBOARDING}/begin")
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
        let _: serde_json::Value = request(
            self.origin(),
            Some(self.config.publishable_key()),
            "POST",
            "/auth/v1/logout?scope=local",
            None,
            Some(bearer),
            None,
            true,
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
        let response: Response = request(
            self.origin(),
            Some(self.config.publishable_key()),
            "POST",
            &format!("/auth/v1/token?grant_type={grant}"),
            Some(body),
            None,
            None,
            true,
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
        request(
            self.origin(),
            Some(self.config.publishable_key()),
            "POST",
            &format!("{ONBOARDING}/{action}"),
            Some(body.clone()),
            bearer,
            None,
            is_mutating,
        )
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

fn post_send_error(
    kind: AdapterErrorKind,
    is_mutating: bool,
    message: &'static str,
) -> DactylError {
    if is_mutating {
        DactylError::adapter_with_code(kind, "outcome_unknown", message)
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
    origin: &str,
    key: Option<&str>,
    method: &str,
    path: &str,
    body: Option<serde_json::Value>,
    bearer: Option<&str>,
    scm: Option<&ScmCredential>,
    is_mutating: bool,
) -> Result<T, DactylError> {
    validate_origin(origin)?;
    let endpoint = format!("{}{path}", origin.trim_end_matches('/'));
    let method = reqwest::Method::from_bytes(method.as_bytes())
        .map_err(|_| protocol("invalid auth request method"))?;
    let header = |value: &str| -> Result<reqwest::header::HeaderValue, DactylError> {
        let mut header = reqwest::header::HeaderValue::from_str(value)
            .map_err(|_| protocol("invalid authentication header"))?;
        header.set_sensitive(true);
        Ok(header)
    };
    let key = key.map(header).transpose()?;
    let bearer = bearer
        .map(|value| {
            opaque(value)?;
            header(&format!("Bearer {value}"))
        })
        .transpose()?;
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
    std::thread::Builder::new()
        .name("dactyl-cloud-auth".into())
        .spawn(move || {
            let client = reqwest::blocking::Client::builder()
                .connect_timeout(Duration::from_secs(5))
                .timeout(Duration::from_secs(15))
                .redirect(reqwest::redirect::Policy::none())
                .retry(reqwest::retry::never())
                .build()
                .map_err(|_| {
                    DactylError::adapter(
                        AdapterErrorKind::Unavailable,
                        "cloud authentication transport failed",
                    )
                })?;
            let mut request = client
                .request(method, endpoint)
                .header("Accept", "application/json");
            if let Some(key) = key {
                request = request.header("apikey", key);
            }
            if let Some(bearer) = bearer {
                request = request.header(reqwest::header::AUTHORIZATION, bearer);
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
            let response = request.send().map_err(|e| {
                if e.is_timeout() {
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
                }
            })?;
            let status = response.status();
            let mut bytes = Vec::new();
            response
                .take(BODY_LIMIT + 1)
                .read_to_end(&mut bytes)
                .map_err(|error| {
                    if crate::adapter::supabase::body_read_timed_out(&error) {
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
                    }
                })?;
            if bytes.len() as u64 > BODY_LIMIT {
                return Err(post_send_error(
                    AdapterErrorKind::Protocol,
                    is_mutating,
                    "auth response exceeds body limit",
                ));
            }
            if !status.is_success() {
                let kind = match status.as_u16() {
                    400 | 401 => AdapterErrorKind::Authentication,
                    403 => AdapterErrorKind::Authorization,
                    409 => AdapterErrorKind::Conflict,
                    429 => AdapterErrorKind::RateLimited,
                    500..=599 => AdapterErrorKind::Unavailable,
                    _ => AdapterErrorKind::Protocol,
                };
                let envelope = check_json_depth(&bytes)
                    .then(|| serde_json::from_slice::<serde_json::Value>(&bytes).ok())
                    .flatten();
                if let Some(code) = envelope.as_ref().and_then(public_error_code) {
                    return Err(DactylError::adapter_with_code(
                        kind,
                        code,
                        "cloud request was rejected",
                    ));
                }
                return Err(DactylError::adapter(
                    kind,
                    "cloud authentication request was rejected",
                ));
            }
            if !check_json_depth(&bytes) {
                return Err(post_send_error(
                    AdapterErrorKind::Protocol,
                    is_mutating,
                    "cloud response exceeded JSON nesting limit",
                ));
            }
            serde_json::from_slice(if bytes.is_empty() { b"null" } else { &bytes }).map_err(|_| {
                post_send_error(
                    AdapterErrorKind::Protocol,
                    is_mutating,
                    "invalid cloud authentication response",
                )
            })
        })
        .map_err(|_| protocol("auth worker unavailable"))?
        .join()
        .map_err(|_| protocol("auth worker failed"))?
}
