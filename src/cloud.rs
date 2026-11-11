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

    /// Production uses one built-in origin. An explicit override supports
    /// controlled test/private deployments without changing project policy.
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

    pub fn start_authentication(
        &self,
        repository: &str,
        secrets: &AuthenticationSecrets,
    ) -> Result<AuthenticationStart, DactylError> {
        secrets.validate()?;
        let response: AuthenticationStart = self.auth_request(
            "start",
            &serde_json::json!({
                "version": 1, "repository": repository,
                "pkce_challenge": AuthenticationSecrets::challenge(&secrets.verifier),
                "poll_challenge": AuthenticationSecrets::challenge(&secrets.poll_secret)
            }),
            None,
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
            self.auth_request("status", &flow_body(flow_id, secrets)?, None)?;
        match response.get("status").and_then(|v| v.as_str()) {
            Some(status @ ("pending" | "ready" | "consumed" | "complete")) => Ok(status.into()),
            _ => Err(protocol("invalid authentication status")),
        }
    }

    /// One-time operation. The caller must persist an in-flight marker before
    /// calling; a lost response cannot be retried as if it never committed.
    pub fn exchange_authentication_code(
        &self,
        flow_id: &str,
        secrets: &AuthenticationSecrets,
    ) -> Result<String, DactylError> {
        let response: serde_json::Value =
            self.auth_request("exchange", &flow_body(flow_id, secrets)?, None)?;
        let code = response
            .get("auth_code")
            .and_then(|v| v.as_str())
            .ok_or_else(|| protocol("authentication exchange returned no code"))?;
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
    ) -> Result<(), DactylError> {
        let result: serde_json::Value =
            self.auth_request("complete", &flow_body(flow_id, secrets)?, Some(bearer))?;
        if result.get("status").and_then(|v| v.as_str()) != Some("complete") {
            return Err(protocol("authentication completion was not confirmed"));
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
        }
        let response: Response = request(
            self.origin(),
            Some(self.config.publishable_key()),
            "POST",
            &format!("/auth/v1/token?grant_type={grant}"),
            Some(body),
            None,
        )?;
        opaque(&response.access_token)?;
        opaque(&response.refresh_token)?;
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

fn request<T: for<'de> Deserialize<'de> + Send + 'static>(
    origin: &str,
    key: Option<&str>,
    method: &str,
    path: &str,
    body: Option<serde_json::Value>,
    bearer: Option<&str>,
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
            let transport = || {
                DactylError::adapter(
                    AdapterErrorKind::Unavailable,
                    "cloud authentication transport failed",
                )
            };
            let client = reqwest::blocking::Client::builder()
                .connect_timeout(Duration::from_secs(5))
                .timeout(Duration::from_secs(15))
                .redirect(reqwest::redirect::Policy::none())
                .build()
                .map_err(|_| transport())?;
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
            let response = request.send().map_err(|e| {
                if e.is_timeout() {
                    DactylError::adapter(
                        AdapterErrorKind::Timeout,
                        "cloud authentication timed out",
                    )
                } else {
                    transport()
                }
            })?;
            let status = response.status();
            let mut bytes = Vec::new();
            response
                .take(BODY_LIMIT + 1)
                .read_to_end(&mut bytes)
                .map_err(|_| transport())?;
            if bytes.len() as u64 > BODY_LIMIT {
                return Err(protocol("auth response exceeds body limit"));
            }
            if !status.is_success() {
                let kind = match status.as_u16() {
                    400 | 401 => AdapterErrorKind::Authentication,
                    403 => AdapterErrorKind::Authorization,
                    429 => AdapterErrorKind::RateLimited,
                    500..=599 => AdapterErrorKind::Unavailable,
                    _ => AdapterErrorKind::Protocol,
                };
                return Err(DactylError::adapter(
                    kind,
                    "cloud authentication request was rejected",
                ));
            }
            serde_json::from_slice(if bytes.is_empty() { b"null" } else { &bytes })
                .map_err(|_| protocol("invalid cloud authentication response"))
        })
        .map_err(|_| protocol("auth worker unavailable"))?
        .join()
        .map_err(|_| protocol("auth worker failed"))?
}
