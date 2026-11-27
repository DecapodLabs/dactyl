//! Bounded, runtime-aware HTTP transport pool, RFC 9449 DPoP helpers, and
//! origin/auth-bound effect provenance extraction.
//!
//! Equal transport budgets share an HTTP client pool and its bounded in-flight
//! slots, while a process-wide ceiling prevents aggregate concurrency or file
//! descriptor exhaustion across distinct budget pools. Native async callers use
//! [`reqwest::Client`] directly on their active Tokio runtime without worker
//! thread handoffs; synchronous callers reuse [`reqwest::blocking::Client`]
//! directly when outside Tokio and offload blocking initialization/execution
//! off-runtime only when invoked from inside a Tokio runtime.

use std::collections::VecDeque;
use std::sync::{Arc, Condvar, Mutex, Weak};
use std::time::{Duration, Instant};

#[cfg(feature = "supabase")]
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
#[cfg(feature = "supabase")]
use reqwest::header::HeaderMap;
#[cfg(feature = "supabase")]
use reqwest::header::HeaderValue;
#[cfg(feature = "supabase")]
use sha2::{Digest, Sha256};

#[cfg(feature = "supabase")]
use crate::connector::{
    validate_dpop_nonce, validate_provenance_token, AuthScheme, CloudRecoveryFacts,
    CloudResponseMetadata, DecisionOutcome, EffectProvenance, EffectStatus, ExecutionPhase,
    ObservationSource, OperationAttempt, RecoveryActionCategory, RequestProofInput,
    RequestProofSigner, PROVENANCE_CONTRACT_VERSION,
};
#[cfg(feature = "supabase")]
use crate::OperationKind;
use crate::{AdapterErrorKind, DactylError, ResourceBudget};

const MAX_PROCESS_IN_FLIGHT: usize = 64;
const MAX_LIVE_POOLS: usize = 64;
const MAX_ASYNC_RUNTIMES_PER_POOL: usize = 16;

#[cfg(feature = "supabase")]
const MAX_CLOUD_HEADERS: usize = 16;
#[cfg(feature = "supabase")]
const MAX_HEADER_NAME_BYTES: usize = 64;
#[cfg(feature = "supabase")]
const MAX_HEADER_VALUE_BYTES: usize = 256;
#[cfg(feature = "supabase")]
const MAX_METADATA_TOTAL_BYTES: usize = 4096;

#[derive(Clone, Copy, PartialEq, Eq)]
struct PoolSettings {
    connect_timeout: Duration,
    max_in_flight: usize,
    idle_per_host: usize,
    idle_timeout: Duration,
}

impl From<ResourceBudget> for PoolSettings {
    fn from(budget: ResourceBudget) -> Self {
        Self {
            connect_timeout: budget.connect_timeout(),
            max_in_flight: budget.http_max_in_flight(),
            idle_per_host: budget.http_max_idle_per_host(),
            idle_timeout: budget.http_idle_timeout(),
        }
    }
}

pub(crate) struct SharedHttpClient {
    blocking: Mutex<Option<reqwest::blocking::Client>>,
    async_clients: Mutex<Vec<(Option<tokio::runtime::Id>, reqwest::Client)>>,
    settings: PoolSettings,
    limit: RequestLimit,
}

impl SharedHttpClient {
    #[cfg_attr(not(feature = "supabase"), allow(dead_code))]
    pub(crate) fn matches(&self, budget: ResourceBudget) -> bool {
        self.settings == PoolSettings::from(budget)
    }

    pub(crate) fn acquire_permit(
        &self,
        deadline: Instant,
    ) -> Result<RequestPermit<'_>, DactylError> {
        let scoped_slot = self.limit.acquire(deadline)?;
        let process_slot = PROCESS_LIMIT.acquire(deadline)?;
        Ok(RequestPermit {
            _scoped: scoped_slot,
            _process: process_slot,
        })
    }

    pub(crate) async fn acquire_permit_async(
        &self,
        deadline: Instant,
    ) -> Result<RequestPermit<'_>, DactylError> {
        let scoped_slot = self.limit.acquire_async(deadline).await?;
        let process_slot = PROCESS_LIMIT.acquire_async(deadline).await?;
        Ok(RequestPermit {
            _scoped: scoped_slot,
            _process: process_slot,
        })
    }

    /// Obtain a pooled synchronous [`reqwest::blocking::Client`].
    ///
    /// When called inside a Tokio runtime, builds the blocking client on a
    /// short-lived plain thread so `reqwest::blocking` never panics on nested
    /// runtime initialization.
    pub(crate) fn blocking_client(&self) -> Result<reqwest::blocking::Client, DactylError> {
        let mut guard = self
            .blocking
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(client) = guard.as_ref() {
            return Ok(client.clone());
        }
        let settings = self.settings;
        let build = move || {
            reqwest::blocking::Client::builder()
                .connect_timeout(settings.connect_timeout)
                .pool_max_idle_per_host(settings.idle_per_host)
                .pool_idle_timeout(settings.idle_timeout)
                .redirect(reqwest::redirect::Policy::none())
                .retry(reqwest::retry::never())
                .build()
        };
        let client = if in_tokio_runtime() {
            std::thread::Builder::new()
                .name("dactyl-http-init".into())
                .spawn(build)
                .map_err(|_| {
                    DactylError::adapter(
                        AdapterErrorKind::Unavailable,
                        "local preflight: cloud HTTP transport worker failed",
                    )
                })?
                .join()
                .map_err(|_| {
                    DactylError::adapter(
                        AdapterErrorKind::Unavailable,
                        "local preflight: cloud HTTP transport worker panicked",
                    )
                })?
                .map_err(|_| {
                    DactylError::adapter(
                        AdapterErrorKind::Unavailable,
                        "local preflight: cloud HTTP transport initialization failed",
                    )
                })?
        } else {
            build().map_err(|_| {
                DactylError::adapter(
                    AdapterErrorKind::Unavailable,
                    "local preflight: cloud HTTP transport initialization failed",
                )
            })?
        };
        *guard = Some(client.clone());
        Ok(client)
    }

    /// Obtain a pooled native async [`reqwest::Client`] bound to the active
    /// Tokio runtime (by runtime [`tokio::runtime::Id`]) so concurrent or
    /// sequential test runtimes never share stale hyper connection tasks.
    pub(crate) fn async_client(&self) -> Result<reqwest::Client, DactylError> {
        let runtime_id = tokio::runtime::Handle::try_current()
            .ok()
            .map(|handle| handle.id());
        let mut clients = self
            .async_clients
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some((_, client)) = clients.iter().find(|(id, _)| *id == runtime_id) {
            return Ok(client.clone());
        }
        let client = reqwest::Client::builder()
            .connect_timeout(self.settings.connect_timeout)
            .pool_max_idle_per_host(self.settings.idle_per_host)
            .pool_idle_timeout(self.settings.idle_timeout)
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .build()
            .map_err(|_| {
                DactylError::adapter(
                    AdapterErrorKind::Unavailable,
                    "local preflight: cloud async HTTP transport initialization failed",
                )
            })?;
        if clients.len() >= MAX_ASYNC_RUNTIMES_PER_POOL {
            clients.remove(0);
        }
        clients.push((runtime_id, client.clone()));
        Ok(client)
    }
}

impl Drop for SharedHttpClient {
    fn drop(&mut self) {
        let blocking = self
            .blocking
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take();
        if let Some(client) = blocking {
            // reqwest::blocking::Client::drop panics if dropped on a Tokio
            // runtime thread. Only spawn a helper thread when currently inside
            // a Tokio runtime.
            if in_tokio_runtime() {
                if let Ok(handle) = std::thread::Builder::new()
                    .name("dactyl-http-drop".into())
                    .spawn(move || drop(client))
                {
                    let _ = handle.join();
                }
            } else {
                drop(client);
            }
        }
    }
}

pub(crate) fn in_tokio_runtime() -> bool {
    tokio::runtime::Handle::try_current().is_ok()
}

static CLIENT_CACHE: Mutex<Vec<(PoolSettings, Weak<SharedHttpClient>)>> = Mutex::new(Vec::new());
static PROCESS_LIMIT: RequestLimit = RequestLimit::new(MAX_PROCESS_IN_FLIGHT);

pub(crate) fn acquire_client(budget: ResourceBudget) -> Result<Arc<SharedHttpClient>, DactylError> {
    let settings = PoolSettings::from(budget);
    let mut cache = CLIENT_CACHE
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    cache.retain(|(_, weak)| weak.strong_count() > 0);
    for (cached_settings, weak) in cache.iter() {
        if *cached_settings == settings {
            if let Some(client) = weak.upgrade() {
                return Ok(client);
            }
        }
    }
    if cache.len() >= MAX_LIVE_POOLS {
        return Err(DactylError::adapter(
            AdapterErrorKind::Unavailable,
            "local preflight: cloud HTTP transport has too many live budget pools",
        ));
    }
    let shared = Arc::new(SharedHttpClient {
        blocking: Mutex::new(None),
        async_clients: Mutex::new(Vec::new()),
        settings,
        limit: RequestLimit::new(settings.max_in_flight),
    });
    cache.push((settings, Arc::downgrade(&shared)));
    Ok(shared)
}

pub(crate) struct RequestPermit<'a> {
    _scoped: RequestSlot<'a>,
    _process: RequestSlot<'static>,
}

struct RequestLimit {
    state: Mutex<LimitState>,
    capacity: usize,
}

struct LimitState {
    in_flight: usize,
    next_waiter_id: u64,
    waiters: VecDeque<WaiterEntry>,
}

struct WaiterEntry {
    id: u64,
    granted: bool,
    wake: WaiterWake,
}

#[derive(Clone)]
enum WaiterWake {
    Sync(Arc<(Mutex<bool>, Condvar)>),
    Async(Arc<tokio::sync::Notify>),
}

impl WaiterWake {
    fn notify(self) {
        match self {
            Self::Sync(pair) => {
                let mut granted = pair
                    .0
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                *granted = true;
                pair.1.notify_one();
            }
            Self::Async(notify) => {
                notify.notify_one();
            }
        }
    }
}

impl LimitState {
    const fn new() -> Self {
        Self {
            in_flight: 0,
            next_waiter_id: 0,
            waiters: VecDeque::new(),
        }
    }

    fn release_one(&mut self) -> Option<WaiterWake> {
        if let Some(entry) = self.waiters.iter_mut().find(|entry| !entry.granted) {
            entry.granted = true;
            Some(entry.wake.clone())
        } else {
            self.in_flight = self.in_flight.saturating_sub(1);
            None
        }
    }

    fn cancel_waiter(&mut self, id: u64) -> Option<WaiterWake> {
        let idx = self.waiters.iter().position(|entry| entry.id == id)?;
        let entry = self.waiters.remove(idx)?;
        if entry.granted {
            self.release_one()
        } else {
            None
        }
    }

    fn take_granted(&mut self, id: u64) -> bool {
        if let Some(idx) = self.waiters.iter().position(|entry| entry.id == id) {
            if self.waiters[idx].granted {
                self.waiters.remove(idx);
                return true;
            }
        }
        false
    }
}

struct WaiterGuard<'a> {
    limit: &'a RequestLimit,
    id: u64,
    armed: bool,
}

impl<'a> WaiterGuard<'a> {
    fn new(limit: &'a RequestLimit, id: u64) -> Self {
        Self {
            limit,
            id,
            armed: true,
        }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for WaiterGuard<'_> {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let wake = {
            let mut state = self
                .limit
                .state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            state.cancel_waiter(self.id)
        };
        if let Some(wake) = wake {
            wake.notify();
        }
    }
}

fn slot_timeout_error() -> DactylError {
    DactylError::adapter(
        AdapterErrorKind::Timeout,
        "local preflight: cloud HTTP request timed out awaiting bounded connection slot",
    )
}

impl RequestLimit {
    const fn new(capacity: usize) -> Self {
        Self {
            state: Mutex::new(LimitState::new()),
            capacity,
        }
    }

    fn acquire(&self, deadline: Instant) -> Result<RequestSlot<'_>, DactylError> {
        let (id, pair) = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if state.waiters.is_empty() && state.in_flight < self.capacity {
                state.in_flight += 1;
                return Ok(RequestSlot(self));
            }
            if deadline.saturating_duration_since(Instant::now()).is_zero() {
                return Err(slot_timeout_error());
            }
            let id = state.next_waiter_id;
            state.next_waiter_id = state.next_waiter_id.wrapping_add(1);
            let pair = Arc::new((Mutex::new(false), Condvar::new()));
            state.waiters.push_back(WaiterEntry {
                id,
                granted: false,
                wake: WaiterWake::Sync(pair.clone()),
            });
            (id, pair)
        };
        let mut guard = WaiterGuard::new(self, id);
        {
            let mut granted = pair
                .0
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            while !*granted {
                let remaining = deadline.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    return Err(slot_timeout_error());
                }
                let (next, _) = pair
                    .1
                    .wait_timeout(granted, remaining)
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                granted = next;
            }
        }
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if state.take_granted(id) {
            guard.disarm();
            Ok(RequestSlot(self))
        } else {
            Err(slot_timeout_error())
        }
    }

    async fn acquire_async(&self, deadline: Instant) -> Result<RequestSlot<'_>, DactylError> {
        let (id, notify) = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if state.waiters.is_empty() && state.in_flight < self.capacity {
                state.in_flight += 1;
                return Ok(RequestSlot(self));
            }
            if deadline.saturating_duration_since(Instant::now()).is_zero() {
                return Err(slot_timeout_error());
            }
            let id = state.next_waiter_id;
            state.next_waiter_id = state.next_waiter_id.wrapping_add(1);
            let notify = Arc::new(tokio::sync::Notify::new());
            state.waiters.push_back(WaiterEntry {
                id,
                granted: false,
                wake: WaiterWake::Async(notify.clone()),
            });
            (id, notify)
        };
        let mut guard = WaiterGuard::new(self, id);
        loop {
            let notified = notify.notified();
            let mut notified = std::pin::pin!(notified);
            notified.as_mut().enable();
            {
                let mut state = self
                    .state
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                if state.take_granted(id) {
                    guard.disarm();
                    return Ok(RequestSlot(self));
                }
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(slot_timeout_error());
            }
            if tokio::time::timeout(remaining, notified).await.is_err() {
                let mut state = self
                    .state
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                if state.take_granted(id) {
                    guard.disarm();
                    return Ok(RequestSlot(self));
                }
                return Err(slot_timeout_error());
            }
        }
    }
}

struct RequestSlot<'a>(&'a RequestLimit);

impl Drop for RequestSlot<'_> {
    fn drop(&mut self) {
        let wake = {
            let mut state = self
                .0
                .state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            state.release_one()
        };
        if let Some(wake) = wake {
            wake.notify();
        }
    }
}

pub(crate) enum AsyncBodyError {
    TooLarge,
    Transport(reqwest::Error),
}

pub(crate) async fn read_bounded_body_async(
    mut response: reqwest::Response,
    max_wire_bytes: u64,
) -> Result<Vec<u8>, AsyncBodyError> {
    if response
        .content_length()
        .is_some_and(|declared| declared > max_wire_bytes)
    {
        return Err(AsyncBodyError::TooLarge);
    }
    let mut bytes = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(AsyncBodyError::Transport)? {
        let next_len = (bytes.len() as u64)
            .checked_add(chunk.len() as u64)
            .ok_or(AsyncBodyError::TooLarge)?;
        if next_len > max_wire_bytes {
            return Err(AsyncBodyError::TooLarge);
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes)
}

pub(crate) fn validate_endpoint(endpoint: &str) -> Result<reqwest::Url, DactylError> {
    let url = reqwest::Url::parse(endpoint)
        .map_err(|_| DactylError::Config("invalid HTTP service URL".into()))?;
    // Literal loopback permits disposable offline service tests. Never use
    // DNS resolution or suffix matching to decide whether HTTP is trusted.
    let loopback = url.host_str().is_some_and(|host| {
        host.trim_matches(['[', ']'])
            .parse::<std::net::IpAddr>()
            .is_ok_and(|ip| ip.is_loopback())
    });
    if !(url.scheme() == "https" || (url.scheme() == "http" && loopback))
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(DactylError::Config(
            "service requires an HTTPS URL (HTTP only for literal loopback), without credentials, query or fragment".into(),
        ));
    }
    Ok(url)
}

#[cfg(feature = "supabase")]
pub(crate) fn canonical_url(endpoint: &str) -> Result<String, DactylError> {
    let mut url = reqwest::Url::parse(endpoint)
        .map_err(|_| DactylError::adapter(AdapterErrorKind::Protocol, "invalid request URL"))?;
    url.set_query(None);
    url.set_fragment(None);
    Ok(url.into())
}

#[cfg(feature = "supabase")]
pub(crate) fn access_token_sha256(token: &str) -> String {
    URL_SAFE_NO_PAD.encode(Sha256::digest(token.as_bytes()))
}

#[cfg(feature = "supabase")]
pub(crate) fn body_sha256_prefixed(body: &[u8]) -> String {
    let digest = Sha256::digest(body);
    let mut out = String::with_capacity(7 + 64);
    out.push_str("sha256:");
    for byte in digest {
        use std::fmt::Write as _;
        let _ = write!(&mut out, "{byte:02x}");
    }
    out
}

#[cfg(feature = "supabase")]
pub(crate) fn build_auth_headers(
    method: &str,
    canonical_url: &str,
    token: &str,
    signer: Option<&Arc<dyn RequestProofSigner>>,
    nonce: Option<&str>,
) -> Result<(HeaderValue, Option<HeaderValue>), DactylError> {
    if let Some(nonce) = nonce {
        validate_dpop_nonce(nonce)?;
    }
    match signer {
        None => {
            let mut auth = HeaderValue::from_str(&format!("Bearer {token}"))
                .map_err(|_| DactylError::Config("invalid managed session bearer".into()))?;
            auth.set_sensitive(true);
            Ok((auth, None))
        }
        Some(signer) => {
            let ath = access_token_sha256(token);
            let input = RequestProofInput::new(method, canonical_url, Some(&ath), nonce);
            let proof = signer.sign_request(&input)?;
            match proof.scheme() {
                AuthScheme::Bearer => {
                    let mut auth =
                        HeaderValue::from_str(&format!("Bearer {token}")).map_err(|_| {
                            DactylError::Config("invalid managed session bearer".into())
                        })?;
                    auth.set_sensitive(true);
                    Ok((auth, None))
                }
                AuthScheme::DPoP => {
                    let raw_proof = proof.proof().ok_or_else(|| {
                        DactylError::adapter_with_code(
                            AdapterErrorKind::Authentication,
                            "invalid_dpop_proof",
                            "local preflight: DPoP scheme requires a proof header",
                        )
                    })?;
                    let mut auth =
                        HeaderValue::from_str(&format!("DPoP {token}")).map_err(|_| {
                            DactylError::Config("invalid managed session bearer".into())
                        })?;
                    auth.set_sensitive(true);
                    let mut dpop = HeaderValue::from_str(raw_proof).map_err(|_| {
                        DactylError::adapter_with_code(
                            AdapterErrorKind::Authentication,
                            "invalid_dpop_proof",
                            "local preflight: invalid DPoP proof header",
                        )
                    })?;
                    dpop.set_sensitive(true);
                    Ok((auth, Some(dpop)))
                }
            }
        }
    }
}

#[cfg(feature = "supabase")]
pub(crate) type AttemptHeaderList = Vec<(&'static str, HeaderValue)>;

#[cfg(feature = "supabase")]
pub(crate) fn build_attempt_headers(
    attempt: Option<&OperationAttempt>,
    body: Option<&[u8]>,
    kind: Option<OperationKind>,
) -> Result<(Option<String>, AttemptHeaderList), DactylError> {
    let Some(attempt) = attempt else {
        return Ok((None, Vec::new()));
    };
    validate_provenance_token(
        attempt.attempt_id(),
        "local preflight: invalid attempt identifier",
    )?;
    if let Some(digest) = attempt.request_digest() {
        validate_provenance_token(digest, "local preflight: invalid request digest")?;
    }
    if let Some(scope) = attempt.scope_ref() {
        validate_provenance_token(scope, "local preflight: invalid scope reference")?;
    }
    let resolved_digest = attempt
        .request_digest()
        .map(str::to_owned)
        .or_else(|| body.map(body_sha256_prefixed));
    let mut headers = Vec::with_capacity(6);
    let mut idemp = HeaderValue::from_str(attempt.attempt_id()).map_err(|_| {
        DactylError::adapter_with_code(
            AdapterErrorKind::Protocol,
            "invalid_provenance",
            "local preflight: invalid attempt identifier header",
        )
    })?;
    idemp.set_sensitive(true);
    headers.push(("idempotency-key", idemp.clone()));
    headers.push(("decapod-cloud-attempt-id", idemp));
    headers.push((
        "decapod-cloud-provenance-version",
        HeaderValue::from_static("1"),
    ));
    if let Some(ref digest) = resolved_digest {
        let val = HeaderValue::from_str(digest).map_err(|_| {
            DactylError::adapter_with_code(
                AdapterErrorKind::Protocol,
                "invalid_provenance",
                "local preflight: invalid request digest header",
            )
        })?;
        headers.push(("decapod-cloud-request-digest", val));
    }
    if let Some(kind) = kind {
        let kind_str = match kind {
            OperationKind::Read => "read",
            OperationKind::Write => "write",
            OperationKind::Schema => "schema",
        };
        headers.push((
            "decapod-cloud-operation-kind",
            HeaderValue::from_static(kind_str),
        ));
    }
    if let Some(scope) = attempt.scope_ref() {
        let mut val = HeaderValue::from_str(scope).map_err(|_| {
            DactylError::adapter_with_code(
                AdapterErrorKind::Protocol,
                "invalid_provenance",
                "local preflight: invalid scope reference header",
            )
        })?;
        val.set_sensitive(true);
        headers.push(("decapod-cloud-scope-ref", val));
    }
    Ok((resolved_digest, headers))
}

#[cfg(feature = "supabase")]
pub(crate) type AuthHeaders = (Option<HeaderValue>, Option<HeaderValue>);
#[cfg(feature = "supabase")]
pub(crate) type HttpOutcome<T> = (Result<T, DactylError>, Option<CloudResponseMetadata>);

#[cfg(feature = "supabase")]
pub(crate) fn effective_auth_scheme(auth: &AuthHeaders) -> Option<AuthScheme> {
    if auth.1.is_some() {
        Some(AuthScheme::DPoP)
    } else if auth.0.is_some() {
        Some(AuthScheme::Bearer)
    } else {
        None
    }
}

#[cfg(feature = "supabase")]
pub(crate) fn has_dpop_nonce_challenge(headers: &HeaderMap) -> bool {
    headers
        .get_all(reqwest::header::WWW_AUTHENTICATE)
        .iter()
        .filter_map(|value| bounded_printable_ascii(value, 1024))
        .any(|value| {
            let Some((scheme, mut params)) = value.split_once(' ') else {
                return false;
            };
            if !scheme.eq_ignore_ascii_case("dpop") {
                return false;
            }
            while let Some((name, rest)) = params.trim_start().split_once('=') {
                let name = name.trim_end();
                if name.is_empty() || !name.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_')
                {
                    return false;
                }
                let rest = rest.trim_start();
                let (value, tail) = if let Some(quoted) = rest.strip_prefix('"') {
                    let mut escaped = false;
                    let Some(end) = quoted.bytes().position(|byte| {
                        if escaped {
                            escaped = false;
                            return false;
                        }
                        escaped = byte == b'\\';
                        byte == b'"'
                    }) else {
                        return false;
                    };
                    (&quoted[..end], &quoted[end + 1..])
                } else {
                    let end = rest.find(',').unwrap_or(rest.len());
                    (rest[..end].trim_end(), &rest[end..])
                };
                if name.eq_ignore_ascii_case("error") {
                    return value == "use_dpop_nonce"
                        && (tail.trim_start().is_empty() || tail.trim_start().starts_with(','));
                }
                let Some(tail) = tail.trim_start().strip_prefix(',') else {
                    return false;
                };
                params = tail;
            }
            false
        })
}

#[cfg(feature = "supabase")]
fn can_retry_dpop_nonce<T>(first: &HttpOutcome<T>, unauthorized: bool, auth: &AuthHeaders) -> bool {
    let effect_occurred_or_uncertain = first
        .0
        .as_ref()
        .err()
        .is_some_and(|e| e.mutation_outcome() == crate::MutationOutcome::Uncertain)
        || first
            .1
            .as_ref()
            .and_then(CloudResponseMetadata::provenance)
            .is_some_and(|p| {
                matches!(
                    p.status(),
                    EffectStatus::Uncertain | EffectStatus::CommittedObserved
                )
            })
        || first
            .1
            .as_ref()
            .and_then(CloudResponseMetadata::recovery)
            .is_some_and(|r| {
                r.action_category() == RecoveryActionCategory::OutcomeUnknown
                    || r.execution_phase() == ExecutionPhase::AfterSend
            });
    unauthorized
        && first.0.as_ref().err().and_then(DactylError::adapter_code) == Some("use_dpop_nonce")
        && auth.1.is_some()
        && !effect_occurred_or_uncertain
}

/// Retry only a server's explicit pre-execution challenge to a DPoP request.
/// Both attempts share the caller's deadline and use the same target and body.
#[cfg(feature = "supabase")]
pub(crate) fn with_dpop_nonce_retry<T>(
    method: &str,
    canonical_url: &str,
    signer: Option<&Arc<dyn RequestProofSigner>>,
    auth: AuthHeaders,
    managed_bearer_proof: bool,
    mut send: impl FnMut(&AuthHeaders, &mut bool) -> HttpOutcome<T>,
) -> (HttpOutcome<T>, bool) {
    let mut unauthorized = false;
    let first = send(&auth, &mut unauthorized);
    if !can_retry_dpop_nonce(&first, unauthorized, &auth) {
        return (first, false);
    }
    let Some(nonce) = first.1.as_ref().and_then(CloudResponseMetadata::dpop_nonce) else {
        return (first, false);
    };
    let expected_prefix = if managed_bearer_proof {
        "Bearer "
    } else {
        "DPoP "
    };
    let Some(token) = auth
        .0
        .as_ref()
        .and_then(|header| header.to_str().ok())
        .and_then(|value| value.strip_prefix(expected_prefix))
    else {
        return (first, false);
    };
    match build_auth_headers(method, canonical_url, token, signer, Some(nonce)) {
        Ok((next_auth, Some(proof))) => {
            let retry_auth = if managed_bearer_proof {
                auth.0
            } else {
                Some(next_auth)
            };
            (send(&(retry_auth, Some(proof)), &mut unauthorized), true)
        }
        Ok((_, None)) => (first, false),
        Err(error) => ((Err(error), first.1), false),
    }
}

/// Async counterpart to [`with_dpop_nonce_retry`].
#[cfg(feature = "supabase")]
pub(crate) async fn with_dpop_nonce_retry_async<T, F, Fut>(
    method: &str,
    canonical_url: &str,
    signer: Option<&Arc<dyn RequestProofSigner>>,
    auth: AuthHeaders,
    managed_bearer_proof: bool,
    mut send: F,
) -> (HttpOutcome<T>, bool)
where
    F: FnMut(AuthHeaders) -> Fut,
    Fut: std::future::Future<Output = (HttpOutcome<T>, bool)>,
{
    let (first, unauthorized) = send((auth.0.clone(), auth.1.clone())).await;
    if !can_retry_dpop_nonce(&first, unauthorized, &auth) {
        return (first, false);
    }
    let Some(nonce) = first.1.as_ref().and_then(CloudResponseMetadata::dpop_nonce) else {
        return (first, false);
    };
    let expected_prefix = if managed_bearer_proof {
        "Bearer "
    } else {
        "DPoP "
    };
    let Some(token) = auth
        .0
        .as_ref()
        .and_then(|header| header.to_str().ok())
        .and_then(|value| value.strip_prefix(expected_prefix))
    else {
        return (first, false);
    };
    match build_auth_headers(method, canonical_url, token, signer, Some(nonce)) {
        Ok((next_auth, Some(proof))) => {
            let retry_auth = if managed_bearer_proof {
                auth.0
            } else {
                Some(next_auth)
            };
            let (outcome, _) = send((retry_auth, Some(proof))).await;
            (outcome, true)
        }
        Ok((_, None)) => (first, false),
        Err(error) => ((Err(error), first.1), false),
    }
}

#[cfg(feature = "supabase")]
fn bounded_printable_ascii(value: &HeaderValue, max_len: usize) -> Option<String> {
    let text = value.to_str().ok()?.trim();
    if text.is_empty() || text.len() > max_len || !text.bytes().all(|b| (0x20..=0x7E).contains(&b))
    {
        return None;
    }
    Some(text.to_owned())
}

#[cfg(feature = "supabase")]
pub(crate) fn extract_response_metadata(
    headers: &HeaderMap,
    canonical_url: &str,
    signer: Option<&Arc<dyn RequestProofSigner>>,
) -> CloudResponseMetadata {
    let mut metadata = CloudResponseMetadata::default();
    let nonce_values = headers.get_all("dpop-nonce");
    let mut nonces = nonce_values.iter();
    if let Some(raw_nonce) = nonces.next().filter(|_| nonces.next().is_none()) {
        if let Ok(nonce) = raw_nonce.to_str() {
            let trimmed = nonce.trim();
            if validate_dpop_nonce(trimmed).is_ok() {
                metadata.set_dpop_nonce(trimmed.to_owned());
                if let Some(signer) = signer {
                    signer.observe_nonce(canonical_url, trimmed);
                }
            }
        }
    }
    if let Some(sunset) = headers
        .get("sunset")
        .and_then(|value| bounded_printable_ascii(value, MAX_HEADER_VALUE_BYTES))
    {
        metadata.set_sunset(sunset);
    }
    if let Some(deprecation) = headers
        .get("deprecation")
        .and_then(|value| bounded_printable_ascii(value, MAX_HEADER_VALUE_BYTES))
    {
        metadata.set_deprecation(deprecation);
    }
    if let Some(retry_after) = headers
        .get(reqwest::header::RETRY_AFTER)
        .and_then(|value| bounded_printable_ascii(value, MAX_HEADER_VALUE_BYTES))
    {
        metadata.set_retry_after(retry_after);
    }
    let mut total_bytes = 0usize;
    for (name, value) in headers.iter() {
        if metadata.cloud_headers().len() >= MAX_CLOUD_HEADERS {
            break;
        }
        let lower = name.as_str();
        if !lower.starts_with("decapod-cloud-") || lower.len() > MAX_HEADER_NAME_BYTES {
            continue;
        }
        if let Some(val) = bounded_printable_ascii(value, MAX_HEADER_VALUE_BYTES) {
            let entry_bytes = lower.len() + val.len();
            if total_bytes.saturating_add(entry_bytes) > MAX_METADATA_TOTAL_BYTES {
                break;
            }
            total_bytes += entry_bytes;
            metadata.push_cloud_header(lower.to_owned(), val);
        }
    }
    metadata
}

#[cfg(feature = "supabase")]
pub(crate) fn allowlisted_cloud_code(raw: &str) -> Option<&'static str> {
    match raw {
        "authentication_required" => Some("authentication_required"),
        "invalid_token" => Some("invalid_token"),
        "invalid_grant" => Some("invalid_grant"),
        "bad_jwt" => Some("bad_jwt"),
        "session_expired" => Some("session_expired"),
        "session_revoked" => Some("session_revoked"),
        "session_not_found" => Some("session_not_found"),
        "expired_credential" => Some("expired_credential"),
        "refresh_token_not_found" => Some("refresh_token_not_found"),
        "refresh_token_already_used" => Some("refresh_token_already_used"),
        "use_dpop_nonce" => Some("use_dpop_nonce"),
        "invalid_dpop_proof" => Some("invalid_dpop_proof"),
        "invalid_binding" => Some("invalid_binding"),
        "dpop_proof_required" => Some("dpop_proof_required"),
        "missing_dpop_proof" => Some("missing_dpop_proof"),
        "dpop_replay" => Some("dpop_replay"),
        "dpop_nonce_mismatch" => Some("dpop_nonce_mismatch"),
        "dpop_clock_skew" => Some("dpop_clock_skew"),
        "dpop_ath_mismatch" => Some("dpop_ath_mismatch"),
        "dpop_htu_mismatch" => Some("dpop_htu_mismatch"),
        "dpop_htm_mismatch" => Some("dpop_htm_mismatch"),
        "dpop_jkt_mismatch" => Some("dpop_jkt_mismatch"),
        "dpop_aud_mismatch" => Some("dpop_aud_mismatch"),
        "stolen_bearer" => Some("stolen_bearer"),
        "device_revoked" => Some("device_revoked"),
        "device_key_revoked" => Some("device_key_revoked"),
        "device_key_not_found" => Some("device_key_not_found"),
        "device_key_expired" => Some("device_key_expired"),
        "device_session_required" => Some("device_session_required"),
        "device_session_revoked" => Some("device_session_revoked"),
        "challenge_consumed" => Some("challenge_consumed"),
        "challenge_expired" => Some("challenge_expired"),
        "refresh_reuse_detected" => Some("refresh_reuse_detected"),
        "device_not_enrolled" => Some("device_not_enrolled"),
        "authorization_denied" => Some("authorization_denied"),
        "mfa_step_up_required" => Some("mfa_step_up_required"),
        "aal2_step_up_required" => Some("aal2_step_up_required"),
        "aal2_required" => Some("aal2_required"),
        "stepup_required" => Some("stepup_required"),
        "provider_link_stepup_required" => Some("provider_link_stepup_required"),
        "approval_required" => Some("approval_required"),
        "provider_disabled" => Some("provider_disabled"),
        "repo_access_denied" => Some("repo_access_denied"),
        "repository_not_authorized" => Some("repository_not_authorized"),
        "repo_binding_mismatch" => Some("repo_binding_mismatch"),
        "org_binding_mismatch" => Some("org_binding_mismatch"),
        "grant_required" => Some("grant_required"),
        "grant_not_found" => Some("grant_not_found"),
        "grant_revoked" => Some("grant_revoked"),
        "grant_expired" => Some("grant_expired"),
        "grant_stale_generation" => Some("grant_stale_generation"),
        "grant_session_mismatch" => Some("grant_session_mismatch"),
        "scope_denied" => Some("scope_denied"),
        "scope_required" => Some("scope_required"),
        "path_scope_denied" => Some("path_scope_denied"),
        "resource_scope_denied" => Some("resource_scope_denied"),
        "task_scope_denied" => Some("task_scope_denied"),
        "workspace_scope_denied" => Some("workspace_scope_denied"),
        "custody_scope_denied" => Some("custody_scope_denied"),
        "capability_denied" => Some("capability_denied"),
        "audience_mismatch" => Some("audience_mismatch"),
        "parameter_constraint_denied" => Some("parameter_constraint_denied"),
        "delegation_widening_denied" => Some("delegation_widening_denied"),
        "spoofed_authority_claim" => Some("spoofed_authority_claim"),
        "policy_not_bound" => Some("policy_not_bound"),
        "entitlement_required" => Some("entitlement_required"),
        "entitlement_denied" => Some("entitlement_denied"),
        "organization_seat_required" => Some("organization_seat_required"),
        "organization_already_enrolled" => Some("organization_already_enrolled"),
        "admin_approval_required" => Some("admin_approval_required"),
        "admin_required" => Some("admin_required"),
        "owner_required" => Some("owner_required"),
        "provider_quarantined" => Some("provider_quarantined"),
        "quarantined_provider_link" => Some("quarantined_provider_link"),
        "paid_seat_required" => Some("paid_seat_required"),
        "seat_capacity_exceeded" => Some("seat_capacity_exceeded"),
        "seat_limit" => Some("seat_limit"),
        "seat_limit_reached" => Some("seat_limit_reached"),
        "seat_required" => Some("seat_required"),
        "seat_unassigned" => Some("seat_unassigned"),
        "seat_revoked" => Some("seat_revoked"),
        "seat_available_for_assignment" => Some("seat_available_for_assignment"),
        "repository_capacity_exhausted" => Some("repository_capacity_exhausted"),
        "repository_reservation_exhausted" => Some("repository_reservation_exhausted"),
        "reservation_available_for_binding" => Some("reservation_available_for_binding"),
        "repository_not_approved" => Some("repository_not_approved"),
        "repository_access_denied" => Some("repository_access_denied"),
        "code_host_link_required" => Some("code_host_link_required"),
        "repository_verification_required" => Some("repository_verification_required"),
        "unverified_repository" => Some("unverified_repository"),
        "organization_verification_required" => Some("organization_verification_required"),
        "signup_required" => Some("signup_required"),
        "payment_required" => Some("payment_required"),
        "pending_fulfillment" => Some("pending_fulfillment"),
        "tenant_upgrade_required" => Some("tenant_upgrade_required"),
        "tenant_upgrading" => Some("tenant_upgrading"),
        "tenant_suspended" => Some("tenant_suspended"),
        "tenant_archived" => Some("tenant_archived"),
        "org_recovery_required" => Some("org_recovery_required"),
        "policy_template_conflict" => Some("policy_template_conflict"),
        "provenance_origin_mismatch" => Some("provenance_origin_mismatch"),
        "account_setup_required" => Some("account_setup_required"),
        "identity_link_required" => Some("identity_link_required"),
        "custody_conflict" => Some("custody_conflict"),
        "owner_protected" => Some("owner_protected"),
        "member_not_found" => Some("member_not_found"),
        "invitation_expired" => Some("invitation_expired"),
        "invitation_not_found" => Some("invitation_not_found"),
        "project_not_found" => Some("project_not_found"),
        "repository_not_found" => Some("repository_not_found"),
        "target_not_found" => Some("target_not_found"),
        "attempt_not_found" => Some("attempt_not_found"),
        "quote_confirmation_required" => Some("quote_confirmation_required"),
        "billing_binding_required" => Some("billing_binding_required"),
        "payment_pending" => Some("payment_pending"),
        "billing_not_configured" => Some("billing_not_configured"),
        "billing_unavailable" => Some("billing_unavailable"),
        "provider_unavailable" => Some("provider_unavailable"),
        "service_unavailable" => Some("service_unavailable"),
        "overage_not_enabled" => Some("overage_not_enabled"),
        "paid_overages_disabled" => Some("paid_overages_disabled"),
        "quota_exceeded" => Some("quota_exceeded"),
        "overage_cap_exhausted" => Some("overage_cap_exhausted"),
        "capacity_exceeded" => Some("capacity_exceeded"),
        "result_limit_exceeded" => Some("result_limit_exceeded"),
        "invalid_request" => Some("invalid_request"),
        "invalid_scope" => Some("invalid_scope"),
        "payload_too_large" => Some("payload_too_large"),
        "rate_limited" => Some("rate_limited"),
        "method_not_allowed" => Some("method_not_allowed"),
        "not_found" => Some("not_found"),
        "flow_expired" => Some("flow_expired"),
        "flow_not_found" => Some("flow_not_found"),
        "invalid_flow_state" => Some("invalid_flow_state"),
        "version_conflict" => Some("version_conflict"),
        "revision_conflict" => Some("revision_conflict"),
        "prestate_conflict" => Some("prestate_conflict"),
        "lease_conflict" => Some("lease_conflict"),
        "stale_reconciliation" => Some("stale_reconciliation"),
        "reconciliation_not_ready" => Some("reconciliation_not_ready"),
        "reconciliation_required" => Some("reconciliation_required"),
        "remote_creation_recovery_required" => Some("remote_creation_recovery_required"),
        "effect_not_observed" => Some("effect_not_observed"),
        "request_conflict" => Some("request_conflict"),
        "idempotency_conflict" => Some("idempotency_conflict"),
        "idempotency_in_progress" => Some("idempotency_in_progress"),
        "transaction_aborted" => Some("transaction_aborted"),
        "outcome_integrity_failed" => Some("outcome_integrity_failed"),
        "storage_failure" => Some("storage_failure"),
        "storage_unavailable" => Some("storage_unavailable"),
        "unsupported_operation" => Some("unsupported_operation"),
        "unsupported_query" => Some("unsupported_query"),
        "unsupported_type" => Some("unsupported_type"),
        "unsupported_codec" => Some("unsupported_codec"),
        "unsupported_schema" => Some("unsupported_schema"),
        "unsupported_rpc" => Some("unsupported_rpc"),
        "legacy_endpoint_retired" => Some("legacy_endpoint_retired"),
        "missing_context" => Some("missing_context"),
        "invalid_context" => Some("invalid_context"),
        "malformed_context" => Some("malformed_context"),
        "row_not_found" => Some("row_not_found"),
        "row_conflict" => Some("row_conflict"),
        "constraint_failed" => Some("constraint_failed"),
        "unique_violation" => Some("unique_violation"),
        "not_null_violation" => Some("not_null_violation"),
        "foreign_key_violation" => Some("foreign_key_violation"),
        "busy" => Some("busy"),
        "locked" => Some("locked"),
        "timeout" => Some("timeout"),
        "read_only" => Some("read_only"),
        "accepted_not_committed" => Some("accepted_not_committed"),
        "committed_observed" => Some("committed_observed"),
        "rolled_back" => Some("rolled_back"),
        "not_attempted" => Some("not_attempted"),
        "absent" => Some("absent"),
        "pending" => Some("pending"),
        "in_progress" => Some("in_progress"),
        "outcome_unknown" => Some("outcome_unknown"),
        "unsupported_provenance" => Some("unsupported_provenance"),
        _ => None,
    }
}

/// Map an allowlisted cloud error code and HTTP status to a coarse
/// [`AdapterErrorKind`], returning `None` when the HTTP status fallback should
/// decide.
#[cfg(feature = "supabase")]
pub(crate) fn cloud_error_kind_for_code(status: u16, code: &str) -> Option<AdapterErrorKind> {
    match code {
        "authentication_required"
        | "invalid_token"
        | "invalid_grant"
        | "bad_jwt"
        | "session_expired"
        | "session_revoked"
        | "session_not_found"
        | "expired_credential"
        | "refresh_token_not_found"
        | "refresh_token_already_used"
        | "use_dpop_nonce"
        | "invalid_dpop_proof"
        | "invalid_binding"
        | "dpop_proof_required"
        | "missing_dpop_proof"
        | "dpop_replay"
        | "dpop_nonce_mismatch"
        | "dpop_clock_skew"
        | "dpop_ath_mismatch"
        | "dpop_htu_mismatch"
        | "dpop_htm_mismatch"
        | "dpop_jkt_mismatch"
        | "dpop_aud_mismatch"
        | "stolen_bearer"
        | "device_revoked"
        | "device_key_revoked"
        | "device_key_not_found"
        | "device_key_expired"
        | "device_session_required"
        | "device_session_revoked"
        | "challenge_consumed"
        | "challenge_expired"
        | "refresh_reuse_detected"
        | "device_not_enrolled" => Some(AdapterErrorKind::Authentication),
        "authorization_denied"
        | "mfa_step_up_required"
        | "aal2_step_up_required"
        | "aal2_required"
        | "stepup_required"
        | "provider_link_stepup_required"
        | "approval_required"
        | "provider_disabled"
        | "repo_access_denied"
        | "repository_not_authorized"
        | "repository_access_denied"
        | "repository_not_approved"
        | "repo_binding_mismatch"
        | "org_binding_mismatch"
        | "grant_required"
        | "grant_not_found"
        | "grant_revoked"
        | "grant_expired"
        | "grant_stale_generation"
        | "grant_session_mismatch"
        | "scope_denied"
        | "scope_required"
        | "path_scope_denied"
        | "resource_scope_denied"
        | "task_scope_denied"
        | "workspace_scope_denied"
        | "custody_scope_denied"
        | "capability_denied"
        | "audience_mismatch"
        | "parameter_constraint_denied"
        | "delegation_widening_denied"
        | "spoofed_authority_claim"
        | "policy_not_bound"
        | "entitlement_required"
        | "entitlement_denied"
        | "organization_seat_required"
        | "admin_approval_required"
        | "admin_required"
        | "owner_required"
        | "provider_quarantined"
        | "quarantined_provider_link"
        | "paid_seat_required"
        | "seat_limit"
        | "seat_limit_reached"
        | "seat_required"
        | "seat_unassigned"
        | "seat_revoked"
        | "code_host_link_required"
        | "repository_verification_required"
        | "unverified_repository"
        | "organization_verification_required"
        | "signup_required"
        | "payment_required"
        | "pending_fulfillment"
        | "billing_binding_required"
        | "overage_not_enabled"
        | "paid_overages_disabled"
        | "tenant_upgrade_required"
        | "tenant_suspended"
        | "tenant_archived"
        | "org_recovery_required"
        | "account_setup_required"
        | "identity_link_required"
        | "owner_protected" => Some(AdapterErrorKind::Authorization),
        "idempotency_conflict" => Some(AdapterErrorKind::IdempotencyConflict),
        "idempotency_in_progress" | "reconciliation_not_ready" => {
            Some(AdapterErrorKind::IdempotencyInProgress)
        }
        "version_conflict" | "revision_conflict" => Some(AdapterErrorKind::VersionConflict),
        "transaction_aborted" => Some(AdapterErrorKind::TransactionAborted),
        "row_conflict"
        | "custody_conflict"
        | "prestate_conflict"
        | "lease_conflict"
        | "stale_reconciliation"
        | "request_conflict"
        | "outcome_integrity_failed"
        | "organization_already_enrolled"
        | "policy_template_conflict" => Some(AdapterErrorKind::Conflict),
        "repository_capacity_exhausted"
        | "repository_reservation_exhausted"
        | "seat_capacity_exceeded"
        | "capacity_exceeded" => Some(if matches!(status, 413 | 429) {
            AdapterErrorKind::Quota
        } else if status == 409 {
            AdapterErrorKind::Conflict
        } else {
            AdapterErrorKind::Authorization
        }),
        "tenant_upgrading" => Some(if (500..=599).contains(&status) {
            AdapterErrorKind::Unavailable
        } else {
            AdapterErrorKind::Conflict
        }),
        "payload_too_large"
        | "quota_exceeded"
        | "overage_cap_exhausted"
        | "result_limit_exceeded" => Some(AdapterErrorKind::Quota),
        "rate_limited" => Some(AdapterErrorKind::RateLimited),
        "provenance_origin_mismatch" | "invalid_context" | "malformed_context" => {
            Some(AdapterErrorKind::Protocol)
        }
        "invalid_request" | "method_not_allowed" => Some(AdapterErrorKind::InvalidOperation),
        "unsupported_operation"
        | "unsupported_query"
        | "unsupported_type"
        | "unsupported_codec"
        | "unsupported_schema"
        | "unsupported_rpc"
        | "legacy_endpoint_retired" => Some(AdapterErrorKind::Capability),
        "not_found"
        | "project_not_found"
        | "repository_not_found"
        | "target_not_found"
        | "attempt_not_found"
        | "member_not_found"
        | "invitation_not_found"
        | "flow_not_found"
        | "row_not_found" => Some(AdapterErrorKind::NotFound),
        "constraint_failed"
        | "unique_violation"
        | "not_null_violation"
        | "foreign_key_violation" => Some(AdapterErrorKind::Constraint),
        "busy" => Some(AdapterErrorKind::Busy),
        "locked" => Some(AdapterErrorKind::Locked),
        "timeout" => Some(AdapterErrorKind::Timeout),
        "read_only" => Some(AdapterErrorKind::ReadOnly),
        "storage_failure" => Some(AdapterErrorKind::Storage),
        "storage_unavailable"
        | "provider_unavailable"
        | "billing_unavailable"
        | "service_unavailable" => Some(AdapterErrorKind::Unavailable),
        _ => None,
    }
}

#[cfg(feature = "supabase")]
fn parse_recovery_category(raw: &str) -> Option<RecoveryActionCategory> {
    match raw {
        "recoverable_within_authority" => Some(RecoveryActionCategory::RecoverableWithinAuthority),
        "refresh_reconcile_state" => Some(RecoveryActionCategory::RefreshReconcileState),
        "requires_human_action" => Some(RecoveryActionCategory::RequiresHumanAction),
        "denied_permanently_for_scope" => Some(RecoveryActionCategory::DeniedPermanentlyForScope),
        "outcome_unknown" => Some(RecoveryActionCategory::OutcomeUnknown),
        _ => None,
    }
}

#[cfg(feature = "supabase")]
fn parse_recovery_phase(raw: &str) -> Option<ExecutionPhase> {
    match raw {
        "pre_dispatch" => Some(ExecutionPhase::PreDispatch),
        "after_send" => Some(ExecutionPhase::AfterSend),
        _ => None,
    }
}

#[cfg(feature = "supabase")]
fn parse_bounded_snake_token(
    obj: &serde_json::Map<String, serde_json::Value>,
    key: &str,
) -> Result<Option<String>, ()> {
    match obj.get(key) {
        None | Some(serde_json::Value::Null) => Ok(None),
        Some(serde_json::Value::String(s)) => {
            if s.is_empty()
                || s.len() > 64
                || !s
                    .bytes()
                    .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
            {
                return Err(());
            }
            Ok(Some(s.clone()))
        }
        Some(_) => Err(()),
    }
}

#[cfg(feature = "supabase")]
fn parse_optional_u64(
    obj: &serde_json::Map<String, serde_json::Value>,
    key: &str,
) -> Result<Option<u64>, ()> {
    match obj.get(key) {
        None | Some(serde_json::Value::Null) => Ok(None),
        Some(v) => v.as_u64().map(Some).ok_or(()),
    }
}

/// Parse bounded, privacy-safe Propodus `#268` / `048` recovery facts from an
/// untrusted Cloud JSON error envelope (`error.recovery`, top-level `recovery`,
/// or PostgREST `details.recovery`).
///
/// Supports canonical `classification`/`dispatch_phase` keys as well as
/// `action_category`/`execution_phase` aliases (rejecting conflicting pairs),
/// enforces `version == 1`, and rejects malformed, oversized, or out-of-range
/// fields without fabricating metadata.
#[cfg(feature = "supabase")]
pub(crate) fn parse_error_recovery(envelope: &serde_json::Value) -> Option<CloudRecoveryFacts> {
    let rec = envelope
        .get("error")
        .and_then(|v| v.get("recovery"))
        .or_else(|| envelope.get("recovery"))
        .or_else(|| envelope.get("details").and_then(|v| v.get("recovery")))?;
    let obj = rec.as_object()?;
    let version = obj.get("version").and_then(|v| v.as_u64())?;
    if version != 1 {
        return None;
    }

    let cls = obj
        .get("classification")
        .map(|v| v.as_str().and_then(parse_recovery_category))
        .transpose_opt()?;
    let act = obj
        .get("action_category")
        .map(|v| v.as_str().and_then(parse_recovery_category))
        .transpose_opt()?;
    let action_category = match (cls, act) {
        (Some(a), Some(b)) if a != b => return None,
        (Some(a), _) | (None, Some(a)) => a,
        (None, None) => return None,
    };

    let dp = obj
        .get("dispatch_phase")
        .map(|v| v.as_str().and_then(parse_recovery_phase))
        .transpose_opt()?;
    let ep = obj
        .get("execution_phase")
        .map(|v| v.as_str().and_then(parse_recovery_phase))
        .transpose_opt()?;
    let execution_phase = match (dp, ep) {
        (Some(a), Some(b)) if a != b => return None,
        (Some(a), _) | (None, Some(a)) => a,
        (None, None) => return None,
    };

    let missing_fact = parse_bounded_snake_token(obj, "missing_fact").ok()?;
    let authority_freshness = parse_bounded_snake_token(obj, "authority_freshness").ok()?;
    let grant_generation = parse_optional_u64(obj, "grant_generation").ok()?;
    let session_generation = parse_optional_u64(obj, "session_generation").ok()?;
    let attempt_ref = match obj.get("attempt_ref") {
        None | Some(serde_json::Value::Null) => None,
        Some(serde_json::Value::String(s)) => {
            if s.is_empty()
                || s.len() > 256
                || !s
                    .bytes()
                    .all(|b| b.is_ascii_graphic() && !matches!(b, b'"' | b'\\'))
            {
                return None;
            }
            Some(s.clone())
        }
        Some(_) => return None,
    };
    let retry_after_seconds = match parse_optional_u64(obj, "retry_after_seconds").ok()? {
        None => None,
        Some(secs) if secs <= 3600 => Some(secs),
        Some(_) => return None,
    };

    Some(CloudRecoveryFacts::new(
        1,
        action_category,
        execution_phase,
        missing_fact,
        authority_freshness,
        grant_generation,
        session_generation,
        attempt_ref,
        retry_after_seconds,
    ))
}

#[cfg(feature = "supabase")]
trait TransposeOpt<T> {
    fn transpose_opt(self) -> Option<Option<T>>;
}

#[cfg(feature = "supabase")]
impl<T> TransposeOpt<T> for Option<Option<T>> {
    fn transpose_opt(self) -> Option<Option<T>> {
        match self {
            None => Some(None),
            Some(Some(v)) => Some(Some(v)),
            Some(None) => None,
        }
    }
}

#[cfg(feature = "supabase")]
pub(crate) fn extract_error_recovery(
    envelope: Option<&serde_json::Value>,
    origin_matches: bool,
    metadata_out: &mut CloudResponseMetadata,
) {
    if !origin_matches {
        return;
    }
    if let Some(recovery) = envelope.and_then(parse_error_recovery) {
        metadata_out.set_recovery(recovery);
    }
}

#[cfg(feature = "supabase")]
pub(crate) struct ProvenanceCheckContext<'a> {
    pub expected_origin: &'a reqwest::Url,
    pub response_url: &'a reqwest::Url,
    pub expected_auth: Option<AuthScheme>,
    pub expected_kind: Option<OperationKind>,
    pub attempt: Option<&'a OperationAttempt>,
    pub resolved_digest: Option<&'a str>,
    pub observation_source: ObservationSource,
    pub http_status: u16,
    pub is_mutating: bool,
}

#[cfg(feature = "supabase")]
#[derive(Default)]
struct RawProvenance {
    version: Option<u64>,
    request_id: Option<String>,
    attempt_id: Option<String>,
    request_digest: Option<String>,
    operation_kind: Option<String>,
    decision: Option<String>,
    effect_status: Option<String>,
    decision_ref: Option<String>,
    reason_code: Option<String>,
    auth_scheme: Option<String>,
    origin: Option<String>,
    scope_ref: Option<String>,
    any_present: bool,
}

#[cfg(feature = "supabase")]
fn single_header_ascii(headers: &HeaderMap, name: &str) -> Result<Option<String>, DactylError> {
    let mut values = headers.get_all(name).iter();
    let Some(first) = values.next() else {
        return Ok(None);
    };
    if values.next().is_some() {
        return Err(DactylError::adapter_with_code(
            AdapterErrorKind::Protocol,
            "invalid_provenance",
            "duplicate provenance header",
        ));
    }
    let text = bounded_printable_ascii(first, MAX_HEADER_VALUE_BYTES).ok_or_else(|| {
        DactylError::adapter_with_code(
            AdapterErrorKind::Protocol,
            "invalid_provenance",
            "malformed provenance header value",
        )
    })?;
    Ok(Some(text))
}

#[cfg(feature = "supabase")]
fn json_str_field(
    obj: &serde_json::Map<String, serde_json::Value>,
    key: &str,
) -> Result<Option<String>, DactylError> {
    match obj.get(key) {
        None | Some(serde_json::Value::Null) => Ok(None),
        Some(serde_json::Value::String(s)) => {
            if s.is_empty()
                || s.len() > MAX_HEADER_VALUE_BYTES
                || !s.bytes().all(|b| (0x20..=0x7E).contains(&b))
            {
                return Err(DactylError::adapter_with_code(
                    AdapterErrorKind::Protocol,
                    "invalid_provenance",
                    "malformed provenance field value",
                ));
            }
            Ok(Some(s.clone()))
        }
        Some(_) => Err(DactylError::adapter_with_code(
            AdapterErrorKind::Protocol,
            "invalid_provenance",
            "provenance field must be a string",
        )),
    }
}

#[cfg(feature = "supabase")]
fn parse_header_provenance(headers: &HeaderMap) -> Result<RawProvenance, DactylError> {
    let mut raw = RawProvenance::default();
    if let Some(ver) = single_header_ascii(headers, "decapod-cloud-provenance-version")? {
        raw.any_present = true;
        let parsed = ver.parse::<u64>().map_err(|_| {
            DactylError::adapter_with_code(
                AdapterErrorKind::Protocol,
                "unsupported_provenance_version",
                "invalid provenance version header",
            )
        })?;
        raw.version = Some(parsed);
    }
    for (header_name, target) in [
        ("decapod-cloud-request-id", &mut raw.request_id),
        ("decapod-cloud-attempt-id", &mut raw.attempt_id),
        ("decapod-cloud-request-digest", &mut raw.request_digest),
        ("decapod-cloud-operation-kind", &mut raw.operation_kind),
        ("decapod-cloud-decision", &mut raw.decision),
        ("decapod-cloud-effect-status", &mut raw.effect_status),
        ("decapod-cloud-decision-ref", &mut raw.decision_ref),
        ("decapod-cloud-reason-code", &mut raw.reason_code),
        ("decapod-cloud-auth-scheme", &mut raw.auth_scheme),
        ("decapod-cloud-origin", &mut raw.origin),
        ("decapod-cloud-scope-ref", &mut raw.scope_ref),
    ] {
        if let Some(val) = single_header_ascii(headers, header_name)? {
            // Do not treat bare diagnostic request-id alone as a provenance advertisement
            // unless another provenance field/version is also present.
            if header_name != "decapod-cloud-request-id" {
                raw.any_present = true;
            }
            *target = Some(val);
        }
    }
    Ok(raw)
}

#[cfg(feature = "supabase")]
fn parse_json_provenance(body: Option<&serde_json::Value>) -> Result<RawProvenance, DactylError> {
    let mut raw = RawProvenance::default();
    let Some(envelope) = body.and_then(|v| v.get("provenance")) else {
        return Ok(raw);
    };
    raw.any_present = true;
    let obj = envelope.as_object().ok_or_else(|| {
        DactylError::adapter_with_code(
            AdapterErrorKind::Protocol,
            "invalid_provenance",
            "provenance envelope must be an object",
        )
    })?;
    if let Some(ver) = obj.get("version") {
        let parsed = ver.as_u64().ok_or_else(|| {
            DactylError::adapter_with_code(
                AdapterErrorKind::Protocol,
                "unsupported_provenance_version",
                "provenance version must be an integer",
            )
        })?;
        raw.version = Some(parsed);
    }
    raw.request_id = json_str_field(obj, "request_id")?;
    raw.attempt_id = json_str_field(obj, "attempt_id")?;
    raw.request_digest = json_str_field(obj, "request_digest")?;
    raw.operation_kind = json_str_field(obj, "operation_kind")?;
    raw.decision = json_str_field(obj, "decision")?;
    raw.effect_status = json_str_field(obj, "effect_status")?;
    raw.decision_ref = json_str_field(obj, "decision_ref")?;
    raw.reason_code = json_str_field(obj, "reason_code")?;
    raw.auth_scheme = json_str_field(obj, "auth_scheme")?;
    raw.origin = json_str_field(obj, "origin")?;
    raw.scope_ref = json_str_field(obj, "scope_ref")?;
    Ok(raw)
}

#[cfg(feature = "supabase")]
fn merge_field(left: Option<String>, right: Option<String>) -> Result<Option<String>, DactylError> {
    match (left, right) {
        (Some(a), Some(b)) if a != b => Err(DactylError::adapter_with_code(
            AdapterErrorKind::Protocol,
            "provenance_conflict",
            "HTTP header and JSON body provenance fields disagree",
        )),
        (Some(a), _) => Ok(Some(a)),
        (None, Some(b)) => Ok(Some(b)),
        (None, None) => Ok(None),
    }
}

#[cfg(feature = "supabase")]
fn provenance_fault(
    ctx: &ProvenanceCheckContext<'_>,
    metadata_out: &CloudResponseMetadata,
    code: &str,
    origin_bound: bool,
) -> EffectProvenance {
    let request_id = if origin_bound {
        metadata_out
            .cloud_header("decapod-cloud-request-id")
            .and_then(|id| {
                validate_provenance_token(id, "invalid request id")
                    .ok()
                    .map(|()| id.to_owned())
            })
    } else {
        None
    };
    EffectProvenance::transport_fault(
        ctx.expected_kind,
        ctx.attempt,
        ctx.resolved_digest,
        request_id,
        Some(code),
        ctx.expected_auth,
        origin_bound,
        ctx.expected_origin.scheme() == "https",
    )
}

#[cfg(feature = "supabase")]
pub(crate) fn bind_response_provenance(
    headers: &HeaderMap,
    body_json: Option<&serde_json::Value>,
    ctx: &ProvenanceCheckContext<'_>,
    metadata_out: &mut CloudResponseMetadata,
    fallback_error_code: Option<&str>,
) -> Result<(), DactylError> {
    let tls_authenticated = ctx.expected_origin.scheme() == "https";
    if ctx.response_url.origin() != ctx.expected_origin.origin()
        || (300..400).contains(&ctx.http_status)
    {
        metadata_out.set_provenance(provenance_fault(
            ctx,
            metadata_out,
            "provenance_origin_mismatch",
            false,
        ));
        return Err(DactylError::adapter_with_code(
            AdapterErrorKind::Protocol,
            "provenance_origin_mismatch",
            "response origin did not match authenticated endpoint",
        ));
    }

    let hdr_raw = match parse_header_provenance(headers) {
        Ok(raw) => raw,
        Err(err) => {
            metadata_out.set_provenance(provenance_fault(
                ctx,
                metadata_out,
                err.adapter_code().unwrap_or("invalid_provenance"),
                true,
            ));
            return Err(err);
        }
    };
    let json_raw = match parse_json_provenance(body_json) {
        Ok(raw) => raw,
        Err(err) => {
            metadata_out.set_provenance(provenance_fault(
                ctx,
                metadata_out,
                err.adapter_code().unwrap_or("invalid_provenance"),
                true,
            ));
            return Err(err);
        }
    };

    if !hdr_raw.any_present && !json_raw.any_present {
        metadata_out.set_provenance(EffectProvenance::unsupported(
            ctx.observation_source,
            ctx.expected_kind,
            ctx.attempt,
            ctx.resolved_digest,
            hdr_raw.request_id,
            fallback_error_code.map(str::to_owned),
            ctx.expected_auth,
            true,
            tls_authenticated,
        ));
        return Ok(());
    }

    let version = match (hdr_raw.version, json_raw.version) {
        (Some(a), Some(b)) if a != b => {
            metadata_out.set_provenance(provenance_fault(
                ctx,
                metadata_out,
                "provenance_conflict",
                true,
            ));
            return Err(DactylError::adapter_with_code(
                AdapterErrorKind::Protocol,
                "provenance_conflict",
                "HTTP header and JSON provenance versions disagree",
            ));
        }
        (Some(v), _) | (None, Some(v)) => v,
        (None, None) => {
            metadata_out.set_provenance(provenance_fault(
                ctx,
                metadata_out,
                "invalid_provenance",
                true,
            ));
            return Err(DactylError::adapter_with_code(
                AdapterErrorKind::Protocol,
                "invalid_provenance",
                "provenance metadata requires explicit contract version 1",
            ));
        }
    };
    if version != u64::from(PROVENANCE_CONTRACT_VERSION) {
        metadata_out.set_provenance(provenance_fault(
            ctx,
            metadata_out,
            "unsupported_provenance_version",
            true,
        ));
        return Err(DactylError::adapter_with_code(
            AdapterErrorKind::Protocol,
            "unsupported_provenance_version",
            "unsupported provenance contract version",
        ));
    }

    let merged = (|| -> Result<RawProvenance, DactylError> {
        Ok(RawProvenance {
            version: Some(version),
            request_id: merge_field(hdr_raw.request_id, json_raw.request_id)?,
            attempt_id: merge_field(hdr_raw.attempt_id, json_raw.attempt_id)?,
            request_digest: merge_field(hdr_raw.request_digest, json_raw.request_digest)?,
            operation_kind: merge_field(hdr_raw.operation_kind, json_raw.operation_kind)?,
            decision: merge_field(hdr_raw.decision, json_raw.decision)?,
            effect_status: merge_field(hdr_raw.effect_status, json_raw.effect_status)?,
            decision_ref: merge_field(hdr_raw.decision_ref, json_raw.decision_ref)?,
            reason_code: merge_field(hdr_raw.reason_code, json_raw.reason_code)?,
            auth_scheme: merge_field(hdr_raw.auth_scheme, json_raw.auth_scheme)?,
            origin: merge_field(hdr_raw.origin, json_raw.origin)?,
            scope_ref: merge_field(hdr_raw.scope_ref, json_raw.scope_ref)?,
            any_present: true,
        })
    })();
    let raw = match merged {
        Ok(raw) => raw,
        Err(err) => {
            metadata_out.set_provenance(provenance_fault(
                ctx,
                metadata_out,
                err.adapter_code().unwrap_or("provenance_conflict"),
                true,
            ));
            return Err(err);
        }
    };

    let validated_result = validate_merged_provenance(raw, ctx, tls_authenticated);
    match validated_result {
        Ok(provenance) => {
            let status = provenance.status();
            let reason = provenance.reason_code().map(str::to_owned);
            metadata_out.set_provenance(provenance);
            if (200..300).contains(&ctx.http_status) {
                match status {
                    EffectStatus::Refused => {
                        return Err(DactylError::adapter_with_code(
                            AdapterErrorKind::Authorization,
                            reason.unwrap_or_else(|| "authorization_denied".into()),
                            "server provenance reported refusal",
                        ));
                    }
                    EffectStatus::RolledBack => {
                        return Err(DactylError::adapter_with_code(
                            AdapterErrorKind::TransactionAborted,
                            reason.unwrap_or_else(|| "transaction_aborted".into()),
                            "server provenance reported rollback",
                        ));
                    }
                    EffectStatus::Uncertain
                        if ctx.is_mutating
                            && ctx.observation_source == ObservationSource::DirectResponse =>
                    {
                        return Err(crate::budget::mark_remote_uncertain(
                            DactylError::adapter_with_code(
                                AdapterErrorKind::IdempotencyInProgress,
                                reason.unwrap_or_else(|| "outcome_unknown".into()),
                                "server accepted operation without committing",
                            ),
                        ));
                    }
                    _ => {}
                }
            }
            Ok(())
        }
        Err(err) => {
            let code = err.adapter_code().unwrap_or("invalid_provenance");
            let origin_bound = code != "provenance_origin_mismatch";
            metadata_out.set_provenance(provenance_fault(ctx, metadata_out, code, origin_bound));
            Err(err)
        }
    }
}

#[cfg(feature = "supabase")]
fn validate_merged_provenance(
    raw: RawProvenance,
    ctx: &ProvenanceCheckContext<'_>,
    tls_authenticated: bool,
) -> Result<EffectProvenance, DactylError> {
    for (opt, msg) in [
        (&raw.request_id, "invalid provenance request identifier"),
        (&raw.attempt_id, "invalid provenance attempt identifier"),
        (&raw.request_digest, "invalid provenance request digest"),
        (&raw.decision_ref, "invalid provenance decision reference"),
        (&raw.scope_ref, "invalid provenance scope reference"),
    ] {
        if let Some(val) = opt.as_deref() {
            validate_provenance_token(val, msg)?;
        }
    }

    if let Some(ref origin_str) = raw.origin {
        let parsed = reqwest::Url::parse(origin_str).map_err(|_| {
            DactylError::adapter_with_code(
                AdapterErrorKind::Protocol,
                "provenance_origin_mismatch",
                "invalid provenance origin URL",
            )
        })?;
        if parsed.path() != "/"
            || !parsed.username().is_empty()
            || parsed.password().is_some()
            || parsed.query().is_some()
            || parsed.fragment().is_some()
            || parsed.origin() != ctx.expected_origin.origin()
        {
            return Err(DactylError::adapter_with_code(
                AdapterErrorKind::Protocol,
                "provenance_origin_mismatch",
                "provenance origin did not match authenticated endpoint",
            ));
        }
    }

    if let Some(ref auth_str) = raw.auth_scheme {
        let parsed_auth = match auth_str.as_str() {
            "bearer" => Some(AuthScheme::Bearer),
            "dpop" => Some(AuthScheme::DPoP),
            "none" => None,
            _ => {
                return Err(DactylError::adapter_with_code(
                    AdapterErrorKind::Protocol,
                    "invalid_provenance",
                    "unknown provenance auth_scheme",
                ));
            }
        };
        if parsed_auth != ctx.expected_auth {
            return Err(DactylError::adapter_with_code(
                AdapterErrorKind::Protocol,
                "provenance_auth_mismatch",
                "provenance auth scheme did not match authenticated request scheme",
            ));
        }
    }

    let op_kind = match raw.operation_kind.as_deref() {
        None => ctx.expected_kind,
        Some("read") => Some(OperationKind::Read),
        Some("write") => Some(OperationKind::Write),
        Some("schema") => Some(OperationKind::Schema),
        Some(_) => {
            return Err(DactylError::adapter_with_code(
                AdapterErrorKind::Protocol,
                "invalid_provenance",
                "unknown provenance operation_kind",
            ));
        }
    };
    if ctx.observation_source == ObservationSource::DirectResponse {
        if let (Some(expected), Some(got)) = (ctx.expected_kind, op_kind) {
            if expected != got {
                return Err(DactylError::adapter_with_code(
                    AdapterErrorKind::Protocol,
                    "provenance_operation_mismatch",
                    "provenance operation_kind did not match request",
                ));
            }
        }
    }

    if let Some(attempt) = ctx.attempt {
        match raw.attempt_id.as_deref() {
            None => {
                return Err(DactylError::adapter_with_code(
                    AdapterErrorKind::Protocol,
                    "provenance_missing_attempt_id",
                    "provenance omitted expected attempt_id correlation",
                ));
            }
            Some(got) if got != attempt.attempt_id() => {
                return Err(DactylError::adapter_with_code(
                    AdapterErrorKind::Protocol,
                    "provenance_attempt_mismatch",
                    "provenance attempt_id did not match request",
                ));
            }
            _ => {}
        }
        if let (Some(expected_digest), Some(got_digest)) =
            (ctx.resolved_digest, raw.request_digest.as_deref())
        {
            if expected_digest != got_digest {
                if ctx.observation_source == ObservationSource::ServerReadback {
                    return Err(DactylError::adapter_with_code(
                        AdapterErrorKind::IdempotencyConflict,
                        "idempotency_conflict",
                        "server readback reported a different request digest for attempt_id",
                    ));
                }
                return Err(DactylError::adapter_with_code(
                    AdapterErrorKind::Protocol,
                    "provenance_digest_mismatch",
                    "provenance request_digest did not match request",
                ));
            }
        }
        if let Some(expected_scope) = attempt.scope_ref() {
            match raw.scope_ref.as_deref() {
                Some(got) if got == expected_scope => {}
                _ => {
                    return Err(DactylError::adapter_with_code(
                        AdapterErrorKind::Protocol,
                        "provenance_scope_mismatch",
                        "provenance scope_ref did not match caller scope",
                    ));
                }
            }
        }
    }

    let raw_status = raw.effect_status.as_deref().ok_or_else(|| {
        DactylError::adapter_with_code(
            AdapterErrorKind::Protocol,
            "invalid_provenance",
            "provenance contract requires effect_status",
        )
    })?;
    let mut default_reason_from_status = None;
    let mut status = match raw_status {
        "committed_observed" => EffectStatus::CommittedObserved,
        "refused" => EffectStatus::Refused,
        "rolled_back" => EffectStatus::RolledBack,
        "uncertain" => EffectStatus::Uncertain,
        "accepted_not_committed" | "pending" | "in_progress" => {
            default_reason_from_status = Some(raw_status.to_owned());
            EffectStatus::Uncertain
        }
        "not_attempted" | "absent" => EffectStatus::NotAttempted,
        _ => {
            return Err(DactylError::adapter_with_code(
                AdapterErrorKind::Protocol,
                "invalid_provenance",
                "unknown provenance effect_status",
            ));
        }
    };

    let decision = match raw.decision.as_deref() {
        None => match status {
            EffectStatus::CommittedObserved => Some(DecisionOutcome::Allowed),
            EffectStatus::Refused => Some(DecisionOutcome::Refused),
            _ => None,
        },
        Some("allowed" | "accepted") => Some(DecisionOutcome::Allowed),
        Some("refused" | "denied") => Some(DecisionOutcome::Refused),
        Some(_) => {
            return Err(DactylError::adapter_with_code(
                AdapterErrorKind::Protocol,
                "invalid_provenance",
                "unknown provenance decision",
            ));
        }
    };

    if status == EffectStatus::CommittedObserved && decision == Some(DecisionOutcome::Refused) {
        return Err(DactylError::adapter_with_code(
            AdapterErrorKind::Protocol,
            "provenance_status_conflict",
            "provenance cannot pair refused decision with committed_observed",
        ));
    }

    if (200..300).contains(&ctx.http_status) {
        if status == EffectStatus::NotAttempted
            && ctx.observation_source == ObservationSource::DirectResponse
        {
            return Err(DactylError::adapter_with_code(
                AdapterErrorKind::Protocol,
                "provenance_status_conflict",
                "direct HTTP 2xx response cannot report not_attempted",
            ));
        }
    } else {
        if status == EffectStatus::CommittedObserved
            && ctx.observation_source == ObservationSource::DirectResponse
        {
            return Err(DactylError::adapter_with_code(
                AdapterErrorKind::Protocol,
                "provenance_status_conflict",
                "non-2xx HTTP response cannot attest committed_observed",
            ));
        }
        if ctx.is_mutating
            && matches!(ctx.http_status, 408 | 500..=599)
            && !matches!(
                status,
                EffectStatus::RolledBack | EffectStatus::NotAttempted | EffectStatus::Refused
            )
        {
            status = EffectStatus::Uncertain;
        }
    }

    let reason_code = match raw.reason_code {
        Some(code) => {
            let safe = allowlisted_cloud_code(&code).ok_or_else(|| {
                DactylError::adapter_with_code(
                    AdapterErrorKind::Protocol,
                    "invalid_provenance",
                    "unknown provenance reason_code",
                )
            })?;
            Some(safe.to_owned())
        }
        None => default_reason_from_status,
    };

    Ok(EffectProvenance::validated(
        status,
        decision,
        ctx.observation_source,
        op_kind,
        raw.request_id,
        raw.attempt_id
            .or_else(|| ctx.attempt.map(|a| a.attempt_id().to_owned())),
        raw.request_digest
            .or_else(|| ctx.resolved_digest.map(str::to_owned)),
        raw.decision_ref,
        reason_code,
        raw.scope_ref,
        ctx.expected_auth,
        true,
        tls_authenticated,
    ))
}

#[cfg(test)]
mod pool_tests {
    use super::*;
    use std::future::Future;

    static TEST_SERIALIZER: RequestLimit = RequestLimit::new(1);

    #[test]
    fn equal_budgets_share_slots_and_other_budgets_keep_independent_capacity() {
        let _serial = TEST_SERIALIZER
            .acquire(Instant::now() + Duration::from_secs(10))
            .unwrap();
        let budget = ResourceBudget::v1().with_http_pool_limits(1, 0, Duration::from_secs(1));
        let first = acquire_client(budget).unwrap();
        let same = acquire_client(budget.with_request_timeout(Duration::from_secs(2))).unwrap();
        assert!(Arc::ptr_eq(&first, &same));
        let different =
            acquire_client(budget.with_http_pool_limits(2, 1, Duration::from_secs(1))).unwrap();
        assert!(!Arc::ptr_eq(&first, &different));
        let held = first.acquire_permit(Instant::now()).unwrap();
        let error = same
            .acquire_permit(Instant::now())
            .err()
            .expect("same pool is full");
        assert_eq!(error.adapter_kind(), Some(AdapterErrorKind::Timeout));
        let one = different.acquire_permit(Instant::now()).unwrap();
        let two = different.acquire_permit(Instant::now()).unwrap();
        assert!(different.acquire_permit(Instant::now()).is_err());
        drop((held, one, two));
        assert!(first.acquire_permit(Instant::now()).is_ok());
        assert!(different.acquire_permit(Instant::now()).is_ok());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn async_and_sync_permits_share_budget_capacity_without_starvation() {
        let _serial = TEST_SERIALIZER
            .acquire_async(Instant::now() + Duration::from_secs(10))
            .await
            .unwrap();
        let budget = ResourceBudget::v1().with_http_pool_limits(2, 2, Duration::from_secs(7));
        let client = acquire_client(budget).unwrap();
        let p1 = client
            .acquire_permit_async(Instant::now() + Duration::from_millis(50))
            .await
            .unwrap();
        let p2 = client
            .acquire_permit_async(Instant::now() + Duration::from_millis(50))
            .await
            .unwrap();
        let timed_out = client
            .acquire_permit_async(Instant::now() + Duration::from_millis(20))
            .await
            .err()
            .expect("pool should be saturated");
        assert_eq!(timed_out.adapter_kind(), Some(AdapterErrorKind::Timeout));

        let client_clone = client.clone();
        let (acquired_tx, acquired_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel::<()>();
        let waiter = tokio::spawn(async move {
            let permit = client_clone
                .acquire_permit_async(Instant::now() + Duration::from_secs(1))
                .await
                .unwrap();
            let _ = acquired_tx.send(());
            let _ = release_rx.await;
            drop(permit);
        });
        tokio::task::yield_now().await;
        drop(p1);
        acquired_rx.await.unwrap();
        assert_eq!(
            client
                .acquire_permit_async(Instant::now() + Duration::from_millis(15))
                .await
                .err()
                .and_then(|e| e.adapter_kind()),
            Some(AdapterErrorKind::Timeout)
        );
        let _ = release_tx.send(());
        waiter.await.unwrap();
        drop(p2);
        assert!(client.acquire_permit(Instant::now()).is_ok());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn fifo_order_and_cancellation_handoff_preserve_mixed_sync_async_fairness() {
        let _serial = TEST_SERIALIZER
            .acquire_async(Instant::now() + Duration::from_secs(10))
            .await
            .unwrap();
        let limit = Arc::new(RequestLimit::new(1));
        let order = Arc::new(Mutex::new(Vec::<&'static str>::new()));

        let held = limit
            .acquire(Instant::now() + Duration::from_secs(1))
            .unwrap();

        // Enqueue: sync-1, async-cancelled, async-2, sync-2.
        let l_s1 = limit.clone();
        let o_s1 = order.clone();
        let (s1_queued_tx, s1_queued_rx) = std::sync::mpsc::sync_channel(1);
        let sync1 = std::thread::spawn(move || {
            let _ = s1_queued_tx.send(());
            let slot = l_s1
                .acquire(Instant::now() + Duration::from_secs(2))
                .unwrap();
            o_s1.lock().unwrap().push("sync-1");
            drop(slot);
        });
        s1_queued_rx.recv().unwrap();
        // Wait until sync-1 is actually registered in limit.state.waiters.
        while limit.state.lock().unwrap().waiters.is_empty() {
            std::thread::yield_now();
        }

        // Poll an async waiter once so it registers, then drop it AFTER it receives handoff!
        let mut cancelled_fut =
            Box::pin(limit.acquire_async(Instant::now() + Duration::from_secs(2)));
        let noop_waker = std::task::Waker::noop();
        let mut cx = std::task::Context::from_waker(noop_waker);
        assert!(cancelled_fut.as_mut().poll(&mut cx).is_pending());
        assert_eq!(limit.state.lock().unwrap().waiters.len(), 2);

        let l_a2 = limit.clone();
        let o_a2 = order.clone();
        let async2 = tokio::spawn(async move {
            let slot = l_a2
                .acquire_async(Instant::now() + Duration::from_secs(2))
                .await
                .unwrap();
            o_a2.lock().unwrap().push("async-2");
            drop(slot);
        });
        while limit.state.lock().unwrap().waiters.len() < 3 {
            tokio::task::yield_now().await;
        }

        let l_s2 = limit.clone();
        let o_s2 = order.clone();
        let sync2 = std::thread::spawn(move || {
            let slot = l_s2
                .acquire(Instant::now() + Duration::from_secs(2))
                .unwrap();
            o_s2.lock().unwrap().push("sync-2");
            drop(slot);
        });
        while limit.state.lock().unwrap().waiters.len() < 4 {
            std::thread::yield_now();
        }

        // Release initial slot: sync-1 runs, then hands off to cancelled_fut (not polled yet!).
        drop(held);
        sync1.join().unwrap();
        // Wait briefly so sync-1's drop marks cancelled_fut granted without polling it.
        tokio::time::sleep(Duration::from_millis(15)).await;
        // Dropping cancelled_fut MUST immediately hand off the granted slot to async-2!
        drop(cancelled_fut);

        async2.await.unwrap();
        sync2.join().unwrap();

        assert_eq!(*order.lock().unwrap(), vec!["sync-1", "async-2", "sync-2"]);
        let state = limit.state.lock().unwrap();
        assert_eq!(state.in_flight, 0);
        assert!(state.waiters.is_empty());
    }

    #[test]
    fn process_in_flight_ceiling_and_live_pool_cap_enforce_bounds_and_release_on_drop() {
        let _serial = TEST_SERIALIZER
            .acquire(Instant::now() + Duration::from_secs(10))
            .unwrap();

        // 1. Distinct budget pools enforce MAX_LIVE_POOLS = 64 and reclaim on drop.
        let mut pools = Vec::with_capacity(MAX_LIVE_POOLS);
        for i in 0..MAX_LIVE_POOLS {
            let budget = ResourceBudget::v1().with_http_pool_limits(
                1,
                0,
                Duration::from_millis(100 + i as u64),
            );
            pools.push(acquire_client(budget).unwrap());
        }
        let overflow_budget =
            ResourceBudget::v1().with_http_pool_limits(1, 0, Duration::from_millis(999));
        let overflow = acquire_client(overflow_budget)
            .err()
            .expect("65th distinct live pool must fail closed");
        assert_eq!(overflow.adapter_kind(), Some(AdapterErrorKind::Unavailable));
        assert_eq!(
            overflow.mutation_outcome(),
            crate::MutationOutcome::NotAttempted
        );

        pools.pop();
        pools.push(acquire_client(overflow_budget).unwrap());

        // 2. Aggregate process ceiling MAX_PROCESS_IN_FLIGHT = 64 holds across distinct pools.
        let mut permits = Vec::with_capacity(MAX_PROCESS_IN_FLIGHT);
        for pool in pools.iter().take(MAX_PROCESS_IN_FLIGHT) {
            permits.push(pool.acquire_permit(Instant::now()).unwrap());
        }
        // Drop one pool to free a live-pool slot while keeping its held permit? Wait:
        // RequestPermit borrows the pool, so drop 1 permit & pool, and open a 64th pool
        // with capacity 4 alongside 63 held permits, OR open 63 pools + 1 pool with cap 2!
        drop(permits);
        pools.pop();
        let multi_pool = acquire_client(ResourceBudget::v1().with_http_pool_limits(
            4,
            0,
            Duration::from_secs(9),
        ))
        .unwrap();
        let mut held_permits = Vec::with_capacity(MAX_PROCESS_IN_FLIGHT);
        for pool in &pools {
            held_permits.push(pool.acquire_permit(Instant::now()).unwrap());
        }
        // 63 slots held across `pools`; acquire 1 more from `multi_pool` (64 total).
        held_permits.push(multi_pool.acquire_permit(Instant::now()).unwrap());
        // `multi_pool` still has 3 scoped slots left, yet PROCESS_LIMIT (64) must block!
        let sat_err = multi_pool
            .acquire_permit(Instant::now() + Duration::from_millis(10))
            .err()
            .expect("process-wide ceiling must block 65th slot");
        assert_eq!(sat_err.adapter_kind(), Some(AdapterErrorKind::Timeout));
        assert_eq!(
            sat_err.mutation_outcome(),
            crate::MutationOutcome::NotAttempted
        );

        held_permits.pop();
        assert!(multi_pool.acquire_permit(Instant::now()).is_ok());
    }
}

#[cfg(all(test, feature = "supabase"))]
mod commercial_allowlist_tests {
    use super::{allowlisted_cloud_code, cloud_error_kind_for_code};
    use crate::AdapterErrorKind;

    #[test]
    fn commercial_entitlement_and_disposition_codes_are_allowlisted() {
        for code in [
            "signup_required",
            "payment_required",
            "pending_fulfillment",
            "entitlement_denied",
            "admin_approval_required",
            "organization_already_enrolled",
            "paid_seat_required",
            "seat_capacity_exceeded",
            "repository_capacity_exhausted",
            "repository_reservation_exhausted",
            "reservation_available_for_binding",
            "repository_not_approved",
            "seat_available_for_assignment",
            "repository_access_denied",
            "admin_required",
            "owner_required",
            "provider_quarantined",
            "quarantined_provider_link",
            "tenant_upgrade_required",
            "tenant_upgrading",
            "tenant_suspended",
            "tenant_archived",
            "org_recovery_required",
            "policy_template_conflict",
            "provenance_origin_mismatch",
        ] {
            assert_eq!(allowlisted_cloud_code(code), Some(code));
        }
        assert_eq!(allowlisted_cloud_code("untrusted_arbitrary_code"), None);
        assert_eq!(allowlisted_cloud_code(""), None);
    }

    #[test]
    fn commercial_codes_map_to_expected_adapter_error_kinds() {
        assert_eq!(
            cloud_error_kind_for_code(402, "payment_required"),
            Some(AdapterErrorKind::Authorization)
        );
        assert_eq!(
            cloud_error_kind_for_code(403, "repository_access_denied"),
            Some(AdapterErrorKind::Authorization)
        );
        assert_eq!(
            cloud_error_kind_for_code(403, "tenant_suspended"),
            Some(AdapterErrorKind::Authorization)
        );
        assert_eq!(
            cloud_error_kind_for_code(409, "repository_capacity_exhausted"),
            Some(AdapterErrorKind::Conflict)
        );
        assert_eq!(
            cloud_error_kind_for_code(429, "seat_capacity_exceeded"),
            Some(AdapterErrorKind::Quota)
        );
        assert_eq!(
            cloud_error_kind_for_code(503, "tenant_upgrading"),
            Some(AdapterErrorKind::Unavailable)
        );
        assert_eq!(
            cloud_error_kind_for_code(409, "policy_template_conflict"),
            Some(AdapterErrorKind::Conflict)
        );
        assert_eq!(
            cloud_error_kind_for_code(403, "provenance_origin_mismatch"),
            Some(AdapterErrorKind::Protocol)
        );
    }

    #[test]
    fn error_recovery_facts_parse_both_canonical_and_alias_keys_and_redact_attempt_ref() {
        use super::parse_error_recovery;
        use crate::{ExecutionPhase, RecoveryActionCategory};

        let canonical = serde_json::json!({
            "error": {
                "code": "grant_stale_generation",
                "message": "grant_stale_generation",
                "recovery": {
                    "version": 1,
                    "classification": "refresh_reconcile_state",
                    "dispatch_phase": "pre_dispatch",
                    "missing_fact": "delegated_permission",
                    "authority_freshness": "stale",
                    "grant_generation": 7,
                    "session_generation": 3,
                    "retry_after_seconds": 15
                }
            }
        });
        let facts = parse_error_recovery(&canonical).expect("canonical recovery");
        assert_eq!(facts.version(), 1);
        assert_eq!(
            facts.action_category(),
            RecoveryActionCategory::RefreshReconcileState
        );
        assert_eq!(facts.execution_phase(), ExecutionPhase::PreDispatch);
        assert_eq!(facts.missing_fact(), Some("delegated_permission"));
        assert_eq!(facts.authority_freshness(), Some("stale"));
        assert_eq!(facts.grant_generation(), Some(7));
        assert_eq!(facts.session_generation(), Some(3));
        assert_eq!(facts.attempt_ref(), None);
        assert_eq!(facts.retry_after_seconds(), Some(15));

        let aliased = serde_json::json!({
            "error": {
                "code": "reconciliation_required",
                "recovery": {
                    "version": 1,
                    "action_category": "outcome_unknown",
                    "execution_phase": "after_send",
                    "missing_fact": "uncertain_outcome",
                    "authority_freshness": "unknown",
                    "attempt_ref": "att-secret-ref-268",
                    "retry_after_seconds": 0
                }
            }
        });
        let aliased_facts = parse_error_recovery(&aliased).expect("aliased recovery");
        assert_eq!(
            aliased_facts.action_category(),
            RecoveryActionCategory::OutcomeUnknown
        );
        assert_eq!(aliased_facts.execution_phase(), ExecutionPhase::AfterSend);
        assert_eq!(aliased_facts.attempt_ref(), Some("att-secret-ref-268"));
        assert_eq!(aliased_facts.retry_after_seconds(), Some(0));
        let dbg = format!("{aliased_facts:?}");
        assert!(dbg.contains("has_attempt_ref: true"));
        assert!(!dbg.contains("att-secret-ref-268"));

        // Reject unsupported version, conflicting aliases, out-of-range retry, or unsafe tokens.
        for bad in [
            serde_json::json!({"error":{"recovery":{"version":2,"classification":"outcome_unknown","dispatch_phase":"after_send"}}}),
            serde_json::json!({"error":{"recovery":{"version":1,"classification":"outcome_unknown","action_category":"requires_human_action","dispatch_phase":"after_send"}}}),
            serde_json::json!({"error":{"recovery":{"version":1,"classification":"outcome_unknown","dispatch_phase":"after_send","execution_phase":"pre_dispatch"}}}),
            serde_json::json!({"error":{"recovery":{"version":1,"classification":"outcome_unknown","dispatch_phase":"after_send","retry_after_seconds":3601}}}),
            serde_json::json!({"error":{"recovery":{"version":1,"classification":"outcome_unknown","dispatch_phase":"after_send","attempt_ref":"bad ref\n"}}}),
        ] {
            assert_eq!(parse_error_recovery(&bad), None);
        }
    }
}
