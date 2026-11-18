//! Bounded, Tokio-safe blocking HTTP transport pool and RFC 9449 DPoP header helpers.
//!
//! Each [`SharedHttpClient`] keeps at most `POOL_MAX_IDLE_PER_HOST` persistent
//! sockets per host and bounds concurrent in-flight requests to
//! `MAX_CONCURRENT_REQUESTS` so 250-way bursts stay well below default process
//! descriptor limits while respecting per-call [`crate::ResourceBudget`] deadlines.

use std::sync::{Arc, Condvar, Mutex, Weak};
use std::time::{Duration, Instant};

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use reqwest::header::{HeaderMap, HeaderValue};
use sha2::{Digest, Sha256};

use crate::connector::{
    validate_dpop_nonce, AuthScheme, CloudResponseMetadata, RequestProofInput, RequestProofSigner,
};
use crate::{AdapterErrorKind, DactylError};

const MAX_CONCURRENT_REQUESTS: usize = 32;
const POOL_MAX_IDLE_PER_HOST: usize = 32;
const POOL_IDLE_TIMEOUT: Duration = Duration::from_secs(15);
const MAX_CACHED_CLIENTS: usize = 8;

const MAX_CLOUD_HEADERS: usize = 16;
const MAX_HEADER_NAME_BYTES: usize = 64;
const MAX_HEADER_VALUE_BYTES: usize = 256;
const MAX_METADATA_TOTAL_BYTES: usize = 4096;

pub(crate) struct SharedHttpClient {
    client: Option<reqwest::blocking::Client>,
}

impl SharedHttpClient {
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

static CLIENT_CACHE: Mutex<Vec<(Duration, Weak<SharedHttpClient>)>> = Mutex::new(Vec::new());
static IN_FLIGHT: (Mutex<usize>, Condvar) = (Mutex::new(0), Condvar::new());

pub(crate) fn acquire_client(
    connect_timeout: Duration,
) -> Result<Arc<SharedHttpClient>, DactylError> {
    let mut cache = CLIENT_CACHE
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    cache.retain(|(_, weak)| weak.strong_count() > 0);
    for (cached_timeout, weak) in cache.iter() {
        if *cached_timeout == connect_timeout {
            if let Some(client) = weak.upgrade() {
                return Ok(client);
            }
        }
    }
    let client = reqwest::blocking::Client::builder()
        .connect_timeout(connect_timeout)
        .pool_max_idle_per_host(POOL_MAX_IDLE_PER_HOST)
        .pool_idle_timeout(POOL_IDLE_TIMEOUT)
        .redirect(reqwest::redirect::Policy::none())
        .retry(reqwest::retry::never())
        .build()
        .map_err(|_| {
            DactylError::adapter(
                AdapterErrorKind::Unavailable,
                "cloud HTTP transport initialization failed",
            )
        })?;
    let shared = Arc::new(SharedHttpClient {
        client: Some(client),
    });
    if cache.len() >= MAX_CACHED_CLIENTS {
        cache.remove(0);
    }
    cache.push((connect_timeout, Arc::downgrade(&shared)));
    Ok(shared)
}

pub(crate) struct RequestPermit;

impl Drop for RequestPermit {
    fn drop(&mut self) {
        let (lock, condvar) = &IN_FLIGHT;
        let mut active = lock.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        *active = active.saturating_sub(1);
        condvar.notify_one();
    }
}

pub(crate) fn acquire_permit(deadline: Instant) -> Result<RequestPermit, DactylError> {
    let (lock, condvar) = &IN_FLIGHT;
    let mut active = lock.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
    while *active >= MAX_CONCURRENT_REQUESTS {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(DactylError::adapter(
                AdapterErrorKind::Timeout,
                "cloud HTTP request timed out awaiting bounded connection slot",
            ));
        }
        let (next, _) = condvar
            .wait_timeout(active, remaining)
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        active = next;
    }
    *active += 1;
    Ok(RequestPermit)
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
