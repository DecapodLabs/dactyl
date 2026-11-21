//! Bounded, Tokio-safe blocking HTTP transport pool and RFC 9449 DPoP header helpers.
//!
//! Equal transport budgets share an HTTP client and its in-flight slots.
//! A process-wide ceiling also bounds aggregate dispatch across budget pools.

use std::sync::{Arc, Condvar, Mutex, Weak};
use std::time::{Duration, Instant};

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use reqwest::header::{HeaderMap, HeaderValue};
use sha2::{Digest, Sha256};

use crate::connector::{
    validate_dpop_nonce, AuthScheme, CloudResponseMetadata, RequestProofInput, RequestProofSigner,
};
use crate::{AdapterErrorKind, DactylError, ResourceBudget};

const MAX_PROCESS_IN_FLIGHT: usize = 64;
const MAX_LIVE_POOLS: usize = 64;

const MAX_CLOUD_HEADERS: usize = 16;
const MAX_HEADER_NAME_BYTES: usize = 64;
const MAX_HEADER_VALUE_BYTES: usize = 256;
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
    client: Option<reqwest::blocking::Client>,
    settings: PoolSettings,
    limit: RequestLimit,
}

impl SharedHttpClient {
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

    pub(crate) fn client(&self) -> &reqwest::blocking::Client {
        self.client
            .as_ref()
            .expect("SharedHttpClient used before drop")
    }
}

impl Drop for SharedHttpClient {
    fn drop(&mut self) {
        if let Some(client) = self.client.take() {
            // reqwest::blocking::Client::drop panics if called directly on a Tokio
            // runtime thread. Drop it on a short-lived plain thread and join it so
            // no background threads outlive the handle.
            if let Ok(handle) = std::thread::Builder::new()
                .name("dactyl-http-drop".into())
                .spawn(move || drop(client))
            {
                let _ = handle.join();
            }
        }
    }
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
            "cloud HTTP transport has too many live budget pools",
        ));
    }
    // reqwest::blocking::Client must be constructed, executed, AND dropped
    // outside any caller Tokio runtime so synchronous callers inside
    // `#[tokio::test]` or async runtimes never panic.
    let client = std::thread::Builder::new()
        .name("dactyl-http-init".into())
        .spawn(move || {
            reqwest::blocking::Client::builder()
                .connect_timeout(settings.connect_timeout)
                .pool_max_idle_per_host(settings.idle_per_host)
                .pool_idle_timeout(settings.idle_timeout)
                .redirect(reqwest::redirect::Policy::none())
                .retry(reqwest::retry::never())
                .build()
        })
        .map_err(|_| {
            DactylError::adapter(
                AdapterErrorKind::Unavailable,
                "cloud HTTP transport worker failed",
            )
        })?
        .join()
        .map_err(|_| {
            DactylError::adapter(
                AdapterErrorKind::Unavailable,
                "cloud HTTP transport worker panicked",
            )
        })?
        .map_err(|_| {
            DactylError::adapter(
                AdapterErrorKind::Unavailable,
                "cloud HTTP transport initialization failed",
            )
        })?;
    let shared = Arc::new(SharedHttpClient {
        client: Some(client),
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
    in_flight: Mutex<usize>,
    changed: Condvar,
    capacity: usize,
}

impl RequestLimit {
    const fn new(capacity: usize) -> Self {
        Self {
            in_flight: Mutex::new(0),
            changed: Condvar::new(),
            capacity,
        }
    }

    fn acquire(&self, deadline: Instant) -> Result<RequestSlot<'_>, DactylError> {
        let mut active = self
            .in_flight
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        while *active >= self.capacity {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(DactylError::adapter(
                    AdapterErrorKind::Timeout,
                    "cloud HTTP request timed out awaiting bounded connection slot",
                ));
            }
            let (next, _) = self
                .changed
                .wait_timeout(active, remaining)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            active = next;
        }
        *active += 1;
        Ok(RequestSlot(self))
    }
}

struct RequestSlot<'a>(&'a RequestLimit);

impl Drop for RequestSlot<'_> {
    fn drop(&mut self) {
        let mut active = self
            .0
            .in_flight
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        *active -= 1;
        self.0.changed.notify_one();
    }
}

pub(crate) fn canonical_url(endpoint: &str) -> Result<String, DactylError> {
    let mut url = reqwest::Url::parse(endpoint)
        .map_err(|_| DactylError::adapter(AdapterErrorKind::Protocol, "invalid request URL"))?;
    url.set_query(None);
    url.set_fragment(None);
    Ok(url.into())
}

pub(crate) fn access_token_sha256(token: &str) -> String {
    URL_SAFE_NO_PAD.encode(Sha256::digest(token.as_bytes()))
}

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

fn bounded_printable_ascii(value: &HeaderValue, max_len: usize) -> Option<String> {
    let text = value.to_str().ok()?.trim();
    if text.is_empty() || text.len() > max_len || !text.bytes().all(|b| (0x20..=0x7E).contains(&b))
    {
        return None;
    }
    Some(text.to_owned())
}

pub(crate) fn extract_response_metadata(
    headers: &HeaderMap,
    canonical_url: &str,
    signer: Option<&Arc<dyn RequestProofSigner>>,
) -> CloudResponseMetadata {
    let mut metadata = CloudResponseMetadata::default();
    if let Some(raw_nonce) = headers.get("dpop-nonce") {
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

#[cfg(test)]
mod pool_tests {
    use super::*;

    #[test]
    fn equal_budgets_share_slots_and_other_budgets_keep_independent_capacity() {
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
}
