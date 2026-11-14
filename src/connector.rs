//! Logical local/cloud connector boundary and portable session/account types.
//!
//! Applications select [`LogicalTarget::local`] or [`LogicalTarget::cloud`]
//! per workspace without mutating process-global environment variables. Dactyl
//! resolves local targets to SQLite and cloud targets to the versioned public
//! deployment profile, never falling back to SQLite when cloud resolution or
//! authentication fails.

use serde::{Deserialize, Serialize};

use crate::{
    AdapterErrorKind, Backend, Connection, DactylError, DatastoreRoute, OpenOptions, OperationKind,
    ResourceBudget, StorageContext, SupabaseRpcConfig,
};

#[cfg(feature = "supabase")]
use crate::CloudConnector;

/// Public deployment origin provisioned by the Propodus infrastructure project.
pub const DEFAULT_CLOUD_ORIGIN: &str = "https://goyxpcmxetztjlrrcwew.supabase.co";

pub(crate) fn validate_opaque_credential(
    value: &str,
    message: &'static str,
) -> Result<(), DactylError> {
    if value.is_empty() || value.len() > 16384 || !value.bytes().all(|b| b.is_ascii_graphic()) {
        return Err(DactylError::adapter(AdapterErrorKind::Protocol, message));
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

/// Account and billing operations have fixed backend routes; their versioned
/// payloads, membership, repository authority, seats, pricing, and entitlement
/// decisions remain hosted-service policy.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AccountOperation {
    Register,
    Status,
    Quote,
    Checkout,
    Manage,
    Portal,
}

impl AccountOperation {
    /// Fixed onboarding function segment beneath `/functions/v1/propodus-onboarding/`.
    pub fn action(self) -> &'static str {
        match self {
            Self::Register => "account-register",
            Self::Status => "account-status",
            Self::Quote => "billing-quote",
            Self::Checkout => "billing-checkout",
            Self::Manage => "billing-manage",
            Self::Portal => "billing-portal",
        }
    }

    /// Whether transport loss after dispatch leaves server state uncertain.
    pub fn is_mutating(self) -> bool {
        matches!(
            self,
            Self::Register | Self::Checkout | Self::Manage | Self::Portal
        )
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

/// A user session issued by managed Auth, never a separate Propodus session.
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
    Cloud(CloudConnector),
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
    /// Resolve an explicit [`LogicalTarget`].
    ///
    /// Cloud resolution discovers and validates version 1 of the public
    /// deployment profile without sending user credentials. A failed cloud
    /// discovery or disabled `supabase` feature never falls back to SQLite.
    pub fn resolve(target: LogicalTarget) -> Result<Self, DactylError> {
        match target.inner {
            TargetKind::Local { path } => {
                if path.trim().is_empty() {
                    return Err(DactylError::Config(
                        "local storage path must not be empty".into(),
                    ));
                }
                Ok(Self {
                    inner: ConnectorKind::Local { path },
                })
            }
            TargetKind::Cloud { origin } => {
                #[cfg(feature = "supabase")]
                {
                    Ok(Self {
                        inner: ConnectorKind::Cloud(CloudConnector::discover(&origin)?),
                    })
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

    pub fn local(path: impl Into<String>) -> Result<Self, DactylError> {
        Self::resolve(LogicalTarget::local(path))
    }

    pub fn cloud() -> Result<Self, DactylError> {
        Self::resolve(LogicalTarget::cloud())
    }

    pub fn cloud_with_origin(origin: impl Into<String>) -> Result<Self, DactylError> {
        Self::resolve(LogicalTarget::cloud_with_origin(origin))
    }

    /// Construct a cloud connector from an already verified deployment profile
    /// or offline test fixture without performing network discovery.
    pub fn cloud_with_config(config: SupabaseRpcConfig) -> Result<Self, DactylError> {
        #[cfg(feature = "supabase")]
        {
            Ok(Self {
                inner: ConnectorKind::Cloud(CloudConnector::with_config(config)?),
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
        match &self.inner {
            ConnectorKind::Local { .. } => {
                let _ = (function, request, bearer, kind, budget);
                Err(local_cloud_capability_error())
            }
            #[cfg(feature = "supabase")]
            ConnectorKind::Cloud(connector) => {
                connector.control_request(function, request, bearer, kind, budget)
            }
        }
    }

    /// Transport a hosted account or billing request using the current user
    /// bearer. Customer membership, repository/project authority, seats,
    /// pricing, and entitlements stay on the hosted service; its structured
    /// readiness and denial responses are returned without policy reproduction.
    pub fn account_request(
        &self,
        operation: AccountOperation,
        body: &serde_json::Value,
        bearer: &str,
        scm: Option<&ScmCredential>,
    ) -> Result<serde_json::Value, DactylError> {
        match &self.inner {
            ConnectorKind::Local { .. } => {
                let _ = (operation, body, bearer, scm);
                Err(local_cloud_capability_error())
            }
            #[cfg(feature = "supabase")]
            ConnectorKind::Cloud(connector) => {
                connector.account_request(operation, body, bearer, scm)
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
