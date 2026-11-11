//! Logical cloud connector, public deployment discovery and stateless auth I/O.
//! The application owns browser interaction and private session persistence.

use std::io::Read;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{AdapterErrorKind, DactylError, DatastoreRoute, SupabaseRpcConfig};

/// Public deployment origin provisioned by the Propodus infrastructure project.
pub const DEFAULT_CLOUD_ORIGIN: &str = "https://goyxpcmxetztjlrrcwew.supabase.co";
const ONBOARDING: &str = "/functions/v1/propodus-onboarding";
const BODY_LIMIT: u64 = 16 * 1024;

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct DeploymentProfile {
    version: u16,
    project_origin: String,
    publishable_key: String,
    schema: String,
}

#[derive(Clone, Debug)]
pub struct CloudConnector {
    config: SupabaseRpcConfig,
}

/// Durable pending-flow secrets. Serialize only to the caller's private store.
#[derive(Clone, Serialize, Deserialize)]
pub struct AuthenticationSecrets {
    verifier: String,
    poll_secret: String,
}

impl std::fmt::Debug for AuthenticationSecrets {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("AuthenticationSecrets([redacted])")
    }
}

impl AuthenticationSecrets {
    /// Restore caller-owned pending secrets without generating a new flow.
    pub fn from_parts(verifier: String, poll_secret: String) -> Result<Self, DactylError> {
        let secrets = Self {
            verifier,
            poll_secret,
        };
        secrets.validate()?;
        Ok(secrets)
    }

    pub fn generate() -> Result<Self, DactylError> {
        fn random() -> Result<String, DactylError> {
            let mut bytes = [0; 32];
            getrandom::getrandom(&mut bytes)
                .map_err(|_| protocol("secure randomness unavailable"))?;
            Ok(URL_SAFE_NO_PAD.encode(bytes))
        }
        Ok(Self {
            verifier: random()?,
            poll_secret: random()?,
        })
    }

    fn challenge(value: &str) -> String {
        URL_SAFE_NO_PAD.encode(Sha256::digest(value.as_bytes()))
    }

    fn validate(&self) -> Result<(), DactylError> {
        for value in [&self.verifier, &self.poll_secret] {
            if value.len() != 43
                || URL_SAFE_NO_PAD
                    .decode(value)
                    .map_or(true, |v| v.len() != 32)
            {
                return Err(protocol("invalid pending authentication secrets"));
            }
        }
        Ok(())
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct AuthenticationStart {
    pub flow_id: String,
    pub browser_url: String,
    pub expires_at: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuthenticationState {
    Pending,
    Ready,
    Consumed,
    Complete,
}

/// Browser `Ready` means a code is available, not repository authorization.
/// Callers must compare the repository and expiry with their private binding.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AuthenticationStatus {
    pub status: AuthenticationState,
    pub repository: String,
    pub expires_at: String,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct AuthenticationExchange {
    pub auth_code: String,
    pub repository: String,
}

impl std::fmt::Debug for AuthenticationExchange {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("AuthenticationExchange([redacted])")
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AuthenticationComplete {
    pub repository: String,
}

impl std::fmt::Debug for AuthenticationStart {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("AuthenticationStart([redacted])")
    }
}

/// A user session issued by managed Auth, never a separate Propodus session.
#[derive(Clone, Serialize, Deserialize)]
pub struct UserSession {
    pub access_token: String,
    pub refresh_token: String,
    pub expires_at_unix: u64,
}

impl std::fmt::Debug for UserSession {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("UserSession([redacted])")
    }
}

impl CloudConnector {
    pub fn validate_origin(origin: &str) -> Result<(), DactylError> {
        validate_origin(origin).map(|_| ())
    }

    pub fn rpc_config(&self) -> &SupabaseRpcConfig {
        &self.config
    }

    /// Production uses one built-in origin, independent of process environment.
    /// Explicit `discover(origin)` supports isolated test/private deployments.
    pub fn default_origin() -> String {
        DEFAULT_CLOUD_ORIGIN.into()
    }

    pub fn discover(origin: &str) -> Result<Self, DactylError> {
        let selected = validate_origin(origin)?;
        let profile: DeploymentProfile = request(
            origin,
            None,
            "GET",
            &format!("{ONBOARDING}/config"),
            None,
            None,
            false,
        )?;
        if profile.version != 1
            || profile.schema != "dactyl_api"
            || validate_origin(&profile.project_origin)?.origin() != selected.origin()
        {
            return Err(protocol(
                "cloud deployment profile version or origin mismatch",
            ));
        }
        Self::with_config(
            SupabaseRpcConfig::v1(
                selected.as_str().trim_end_matches('/'),
                profile.publishable_key,
            )
            .with_schema(profile.schema),
        )
    }

    /// Explicit seam for a trusted deployment profile or a disposable fixture.
    pub fn with_config(config: SupabaseRpcConfig) -> Result<Self, DactylError> {
        crate::adapter::supabase::validate_rpc(&config)?;
        Ok(Self { config })
    }

    pub fn from_runtime(origin: &str) -> Result<Self, DactylError> {
        Self::discover(origin)
    }

    pub fn origin(&self) -> &str {
        self.config.project_origin()
    }

    pub fn route(&self, bearer: Option<String>) -> DatastoreRoute {
        DatastoreRoute::supabase_rpc(self.config.clone(), bearer)
    }

    pub fn start_authentication(
        &self,
        repository: &str,
        secrets: &AuthenticationSecrets,
    ) -> Result<AuthenticationStart, DactylError> {
        secrets.validate()?;
        validate_repository(repository)?;
        let response: AuthenticationStart = self.auth_request(
            "start",
            &serde_json::json!({
                "version": 1, "repository": repository,
                "pkce_challenge": AuthenticationSecrets::challenge(&secrets.verifier),
                "poll_challenge": AuthenticationSecrets::challenge(&secrets.poll_secret)
            }),
            None,
        )?;
        self.validate_start(&response)
            .map_err(|_| ambiguous(AdapterErrorKind::Protocol))?;
        Ok(response)
    }

    pub fn validate_start(&self, flow: &AuthenticationStart) -> Result<(), DactylError> {
        validate_flow_id(&flow.flow_id)?;
        validate_expiry(&flow.expires_at)?;
        let browser = reqwest::Url::parse(&flow.browser_url)
            .map_err(|_| protocol("invalid authentication handoff"))?;
        let origin = validate_origin(self.origin())?;
        if browser.origin() != origin.origin()
            || browser.path() != format!("{ONBOARDING}/begin")
            || !browser.username().is_empty()
            || browser.password().is_some()
            || browser.fragment().is_some()
            || browser.query_pairs().count() != 2
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
                .filter(|(k, v)| k == "ticket" && valid_secret(v))
                .count()
                != 1
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
    ) -> Result<AuthenticationStatus, DactylError> {
        let response: AuthenticationStatus =
            self.auth_request("status", &flow_body(flow_id, secrets)?, None)?;
        validate_repository(&response.repository)?;
        validate_expiry(&response.expires_at)?;
        Ok(response)
    }

    /// One-time operation. The caller must persist an in-flight marker before
    /// calling; a lost response cannot be retried as if it never committed.
    pub fn exchange_authentication_code(
        &self,
        flow_id: &str,
        secrets: &AuthenticationSecrets,
    ) -> Result<AuthenticationExchange, DactylError> {
        let response: AuthenticationExchange =
            self.auth_request("exchange", &flow_body(flow_id, secrets)?, None)?;
        opaque(&response.auth_code).map_err(|_| ambiguous(AdapterErrorKind::Protocol))?;
        if !(16..=2048).contains(&response.auth_code.len()) {
            return Err(ambiguous(AdapterErrorKind::Protocol));
        }
        validate_repository(&response.repository)
            .map_err(|_| ambiguous(AdapterErrorKind::Protocol))?;
        Ok(response)
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
            serde_json::json!({"auth_code":code,"code_verifier":secrets.verifier}),
        )
    }

    pub fn refresh_session(&self, refresh_token: &str) -> Result<UserSession, DactylError> {
        opaque(refresh_token)?;
        self.token_request(
            "refresh_token",
            serde_json::json!({"refresh_token":refresh_token}),
        )
    }

    pub fn complete_authentication(
        &self,
        flow_id: &str,
        secrets: &AuthenticationSecrets,
        bearer: &str,
    ) -> Result<AuthenticationComplete, DactylError> {
        #[derive(Deserialize)]
        struct Completion {
            status: String,
            repository: String,
        }
        let result: Completion =
            self.auth_request("complete", &flow_body(flow_id, secrets)?, Some(bearer))?;
        if result.status != "complete" {
            return Err(ambiguous(AdapterErrorKind::Protocol));
        }
        validate_repository(&result.repository)
            .map_err(|_| ambiguous(AdapterErrorKind::Protocol))?;
        Ok(AuthenticationComplete {
            repository: result.repository,
        })
    }

    pub fn revoke_session(&self, bearer: &str) -> Result<(), DactylError> {
        let _: serde_json::Value = request(
            self.origin(),
            Some(self.config.publishable_key()),
            "POST",
            "/auth/v1/logout?scope=local",
            None,
            Some(bearer),
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
            token_type: String,
            expires_in: u64,
            #[serde(default)]
            expires_at: Option<u64>,
        }
        let started = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| protocol("invalid system time"))?
            .as_secs();
        let response: Response = request(
            self.origin(),
            Some(self.config.publishable_key()),
            "POST",
            &format!("/auth/v1/token?grant_type={grant}"),
            Some(body),
            None,
            true,
        )?;
        opaque(&response.access_token).map_err(|_| ambiguous(AdapterErrorKind::Protocol))?;
        opaque(&response.refresh_token).map_err(|_| ambiguous(AdapterErrorKind::Protocol))?;
        if !response.token_type.eq_ignore_ascii_case("bearer")
            || response.expires_in == 0
            || response.expires_in > 86400
        {
            return Err(ambiguous(AdapterErrorKind::Protocol));
        }
        let bound = started
            .checked_add(response.expires_in)
            .ok_or_else(|| ambiguous(AdapterErrorKind::Protocol))?;
        let expires_at_unix = response
            .expires_at
            .map_or(bound, |expiry| expiry.min(bound));
        if expires_at_unix <= started {
            return Err(ambiguous(AdapterErrorKind::Protocol));
        }
        Ok(UserSession {
            access_token: response.access_token,
            refresh_token: response.refresh_token,
            expires_at_unix,
        })
    }

    fn auth_request<T: for<'de> Deserialize<'de> + Send + 'static>(
        &self,
        action: &str,
        body: &serde_json::Value,
        bearer: Option<&str>,
    ) -> Result<T, DactylError> {
        request(
            self.origin(),
            Some(self.config.publishable_key()),
            "POST",
            &format!("{ONBOARDING}/{action}"),
            Some(body.clone()),
            bearer,
            action != "status",
        )
    }
}

fn flow_body(
    flow_id: &str,
    secrets: &AuthenticationSecrets,
) -> Result<serde_json::Value, DactylError> {
    secrets.validate()?;
    validate_flow_id(flow_id)?;
    Ok(serde_json::json!({"version":1,"flow_id":flow_id,"poll_secret":secrets.poll_secret}))
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

fn valid_secret(value: &str) -> bool {
    value.len() == 43
        && URL_SAFE_NO_PAD
            .decode(value)
            .is_ok_and(|bytes| bytes.len() == 32)
}

fn validate_repository(value: &str) -> Result<(), DactylError> {
    if value.is_empty()
        || value.len() > 512
        || value.chars().any(|c| c.is_whitespace() || c.is_control())
    {
        return Err(protocol("invalid authentication repository"));
    }
    Ok(())
}

// The caller owns clock comparison and durable flow deadlines. Validate the
// server's RFC3339 syntax/calendar without guessing or dropping its timezone.
fn validate_expiry(value: &str) -> Result<(), DactylError> {
    fn number(value: &str) -> Option<u32> {
        (!value.is_empty() && value.bytes().all(|b| b.is_ascii_digit()))
            .then(|| value.parse::<u32>().ok())
            .flatten()
    }
    let valid = || -> Option<()> {
        if !value.is_ascii() || value.len() < 20 || value.len() > 40 {
            return None;
        }
        if &value[4..5] != "-"
            || &value[7..8] != "-"
            || &value[10..11] != "T"
            || &value[13..14] != ":"
            || &value[16..17] != ":"
        {
            return None;
        }
        let year = number(&value[0..4])?;
        let month = number(&value[5..7])?;
        let day = number(&value[8..10])?;
        let leap = year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
        let days = match month {
            1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
            4 | 6 | 9 | 11 => 30,
            2 if leap => 29,
            2 => 28,
            _ => return None,
        };
        if year == 0
            || day == 0
            || day > days
            || number(&value[11..13])? > 23
            || number(&value[14..16])? > 59
            || number(&value[17..19])? > 59
        {
            return None;
        }
        let mut suffix = &value[19..];
        if let Some(fraction) = suffix.strip_prefix('.') {
            let length = fraction.bytes().take_while(u8::is_ascii_digit).count();
            if length == 0 || length > 9 {
                return None;
            }
            suffix = &fraction[length..];
        }
        if suffix == "Z" {
            return Some(());
        }
        if suffix.len() != 6
            || !matches!(suffix.as_bytes()[0], b'+' | b'-')
            || &suffix[3..4] != ":"
            || number(&suffix[1..3])? > 23
            || number(&suffix[4..6])? > 59
        {
            return None;
        }
        Some(())
    };
    valid().ok_or_else(|| protocol("invalid authentication expiry"))
}

fn opaque(value: &str) -> Result<(), DactylError> {
    if value.is_empty()
        || value.len() > 16384
        || value.chars().any(|c| c.is_whitespace() || c.is_control())
    {
        return Err(protocol("invalid authentication credential"));
    }
    Ok(())
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

fn ambiguous(kind: AdapterErrorKind) -> DactylError {
    DactylError::adapter_with_code(
        kind,
        "outcome_unknown",
        "cloud mutation outcome is unknown; recover private custody before another attempt",
    )
}

fn response_error(message: &'static str, mutation: bool) -> DactylError {
    if mutation {
        ambiguous(AdapterErrorKind::Protocol)
    } else {
        protocol(message)
    }
}

fn request<T: for<'de> Deserialize<'de> + Send + 'static>(
    origin: &str,
    key: Option<&str>,
    method: &str,
    path: &str,
    body: Option<serde_json::Value>,
    bearer: Option<&str>,
    mutation: bool,
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
    let body = body
        .map(|v| serde_json::to_vec(&v).map_err(|_| protocol("invalid auth request")))
        .transpose()?;
    if body.as_ref().is_some_and(|v| v.len() as u64 > BODY_LIMIT) {
        return Err(protocol("auth request exceeds body limit"));
    }
    std::thread::Builder::new()
        .name("dactyl-cloud-auth".into())
        .spawn(move || {
            let transport = |timeout: bool, submitted: bool| {
                let kind = if timeout {
                    AdapterErrorKind::Timeout
                } else {
                    AdapterErrorKind::Transport
                };
                if mutation && submitted {
                    return ambiguous(kind);
                }
                DactylError::adapter(kind, "cloud authentication transport failed")
            };
            let client = reqwest::blocking::Client::builder()
                .connect_timeout(Duration::from_secs(5))
                .timeout(Duration::from_secs(15))
                .redirect(reqwest::redirect::Policy::none())
                .retry(reqwest::retry::never())
                .build()
                .map_err(|_| transport(false, false))?;
            let mut request = client
                .request(method, endpoint)
                .header("Accept", "application/json");
            if let Some(key) = key {
                request = request.header("apikey", key);
            }
            if let Some(bearer) = bearer {
                request = request.header(reqwest::header::AUTHORIZATION, bearer);
            }
            if let Some(body) = body {
                request = request
                    .header("Content-Type", "application/json")
                    .body(body);
            }
            let response = request
                .send()
                .map_err(|e| transport(e.is_timeout(), true))?;
            let status = response.status();
            let mut bytes = Vec::new();
            response
                .take(BODY_LIMIT + 1)
                .read_to_end(&mut bytes)
                .map_err(|error| {
                    if !status.is_success() {
                        return remote_error(status.as_u16(), &[]);
                    }
                    let timeout = error.kind() == std::io::ErrorKind::TimedOut
                        || error
                            .get_ref()
                            .and_then(|source| source.downcast_ref::<reqwest::Error>())
                            .is_some_and(reqwest::Error::is_timeout);
                    transport(timeout, true)
                })?;
            if bytes.len() as u64 > BODY_LIMIT {
                return Err(if status.is_success() {
                    response_error("auth response exceeds body limit", mutation)
                } else {
                    remote_error(status.as_u16(), &[])
                });
            }
            if !status.is_success() {
                return Err(remote_error(status.as_u16(), &bytes));
            }
            serde_json::from_slice(if bytes.is_empty() { b"null" } else { &bytes })
                .map_err(|_| response_error("invalid cloud authentication response", mutation))
        })
        .map_err(|_| protocol("auth worker unavailable"))?
        .join()
        .map_err(|_| response_error("auth worker failed", mutation))?
}

fn remote_error(status: u16, bytes: &[u8]) -> DactylError {
    let value = serde_json::from_slice::<serde_json::Value>(bytes).ok();
    let code = value.as_ref().and_then(|v| {
        v.get("error_code")
            .and_then(|x| x.as_str())
            .or_else(|| v.get("code").and_then(|x| x.as_str()))
            .or_else(|| {
                v.get("error")
                    .and_then(|x| x.get("code"))
                    .and_then(|x| x.as_str())
            })
            .or_else(|| v.get("error").and_then(|x| x.as_str()))
    });
    let known = match code {
        Some("invalid_token") => Some((AdapterErrorKind::Authentication, "invalid_token")),
        Some("authentication_required") => {
            Some((AdapterErrorKind::Authentication, "authentication_required"))
        }
        Some("session_expired") => Some((AdapterErrorKind::Authentication, "session_expired")),
        Some("session_revoked") => Some((AdapterErrorKind::Authentication, "session_revoked")),
        Some("refresh_token_not_found") => {
            Some((AdapterErrorKind::Authentication, "refresh_token_not_found"))
        }
        Some("refresh_token_already_used") => Some((
            AdapterErrorKind::Authentication,
            "refresh_token_already_used",
        )),
        Some("session_not_found") => Some((AdapterErrorKind::Authentication, "session_not_found")),
        Some("bad_jwt") => Some((AdapterErrorKind::Authentication, "bad_jwt")),
        Some("invalid_grant") => Some((AdapterErrorKind::Authentication, "invalid_grant")),
        Some("user_banned") => Some((AdapterErrorKind::Authorization, "user_banned")),
        Some("authorization_denied") => {
            Some((AdapterErrorKind::Authorization, "authorization_denied"))
        }
        Some("repository_not_authorized") => {
            Some((AdapterErrorKind::Authorization, "repository_not_authorized"))
        }
        Some("entitlement_required") => {
            Some((AdapterErrorKind::Authorization, "entitlement_required"))
        }
        Some("invalid_binding") => Some((AdapterErrorKind::Authorization, "invalid_binding")),
        Some("flow_expired") => Some((AdapterErrorKind::Authentication, "flow_expired")),
        Some("flow_not_found") => Some((AdapterErrorKind::NotFound, "flow_not_found")),
        Some("invalid_flow_state") => Some((AdapterErrorKind::Conflict, "invalid_flow_state")),
        Some("invalid_request") => Some((AdapterErrorKind::InvalidOperation, "invalid_request")),
        Some("rate_limited") => Some((AdapterErrorKind::RateLimited, "rate_limited")),
        Some("service_unavailable") => Some((AdapterErrorKind::Unavailable, "service_unavailable")),
        _ => None,
    };
    if let Some((kind, code)) = known {
        return DactylError::adapter_with_code(
            kind,
            code,
            format!("cloud service rejected request ({code})"),
        );
    }
    let kind = match status {
        400 | 422 => AdapterErrorKind::InvalidOperation,
        401 | 410 => AdapterErrorKind::Authentication,
        402 | 403 => AdapterErrorKind::Authorization,
        404 => AdapterErrorKind::NotFound,
        408 => AdapterErrorKind::Timeout,
        409 => AdapterErrorKind::Conflict,
        413 => AdapterErrorKind::Quota,
        429 => AdapterErrorKind::RateLimited,
        500..=599 => AdapterErrorKind::Unavailable,
        _ => AdapterErrorKind::Protocol,
    };
    DactylError::adapter(kind, format!("cloud service returned HTTP status {status}"))
}
