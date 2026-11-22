//! Authenticated HTTP transport for Decapod Cloud (native Supabase RPC)
//! and explicit Supabase-backed storage service routes.
//!
//! This adapter never connects directly to PostgreSQL or interprets tenant policy.

use std::sync::Arc;
use std::time::Instant;

use serde::{Deserialize, Serialize};

use crate::adapter::{Adapter, AdapterFuture};
use crate::budget::{
    ensure_json_depth, mark_remote_uncertain, preflight_batch, preflight_limit,
    preflight_statement, read_bounded_body, result_limit, serialize_bounded_json, BoundedReadError,
    DecodedResultTracker, ResourceBudget,
};
use crate::connector::{
    validate_dpop_nonce, AuthScheme, CloudResponseMetadata, EffectProvenance, ObservationSource,
    OperationAttempt, RequestProofSigner,
};
use crate::contract::{
    AccessMode, AtomicResult, GeneratedKey, OpenOptions, Operation, OperationKind, OperationResult,
    StorageContext, WriteResult,
};
use crate::error::{AdapterErrorKind, DactylError};
use crate::http_pool::{
    acquire_client, bind_response_provenance, build_attempt_headers, build_auth_headers,
    canonical_url, effective_auth_scheme, extract_response_metadata, has_dpop_nonce_challenge,
    in_tokio_runtime, read_bounded_body_async, with_dpop_nonce_retry, with_dpop_nonce_retry_async,
    AsyncBodyError, AuthHeaders, HttpOutcome, ProvenanceCheckContext, SharedHttpClient,
};
use crate::inspection::ScanOutcome;
use crate::rows::{Parameter, Row, Rows};
use crate::{SupabaseRpcConfig, SUPABASE_RPC_CONTRACT_VERSION};

pub(crate) struct SupabaseAdapter {
    endpoint: String,
    expected_origin: reqwest::Url,
    token: String,
    access_mode: AccessMode,
    context: StorageContext,
    rpc_headers: Option<reqwest::header::HeaderMap>,
    device_gateway: bool,
    budget: ResourceBudget,
    proof_signer: Option<Arc<dyn RequestProofSigner>>,
    dpop_nonce: Option<String>,
    http_client: Arc<SharedHttpClient>,
}

#[derive(Serialize)]
struct Request<'a> {
    sql: &'a str,
    params: &'a [Parameter],
    kind: OperationKind,
    access_mode: AccessMode,
    context: &'a StorageContext,
}

#[derive(Serialize)]
struct BatchRequest<'a> {
    operations: &'a [Operation],
    access_mode: AccessMode,
    context: &'a StorageContext,
}

#[derive(Deserialize)]
struct Response {
    columns: Vec<String>,
    rows: Vec<serde_json::Value>,
    affected_rows: u64,
    #[serde(default)]
    generated_keys: Vec<GeneratedKey>,
}

#[derive(Deserialize)]
struct BatchResponse {
    results: Vec<Response>,
}

pub(crate) fn validate_endpoint(endpoint: &str) -> Result<reqwest::Url, DactylError> {
    let url = reqwest::Url::parse(endpoint)
        .map_err(|_| DactylError::Config("invalid Supabase service URL".into()))?;
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
            "Supabase requires an HTTPS service URL (HTTP only for literal loopback), without credentials, query or fragment".into(),
        ));
    }
    Ok(url)
}

pub(crate) fn validate_rpc(config: &SupabaseRpcConfig) -> Result<reqwest::Url, DactylError> {
    let url = validate_endpoint(config.project_origin())?;
    if url.path() != "/" {
        return Err(DactylError::Config(
            "Supabase RPC requires a project origin without a path".into(),
        ));
    }
    if !config
        .publishable_key()
        .strip_prefix("sb_publishable_")
        .is_some_and(|key| {
            !key.is_empty()
                && key.len() <= 4096
                && key
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
        })
    {
        return Err(DactylError::Config(
            "Supabase RPC requires a publishable project key, never a secret or service-role key"
                .into(),
        ));
    }
    let schema = config.schema().as_bytes();
    if schema.is_empty()
        || schema.len() > 63
        || !(schema[0].is_ascii_alphabetic() || schema[0] == b'_')
        || !schema
            .iter()
            .all(|b| b.is_ascii_alphanumeric() || *b == b'_')
    {
        return Err(DactylError::Config(
            "Supabase RPC requires a valid API schema identifier".into(),
        ));
    }
    Ok(url)
}

// One native header contract for datastore and opaque control calls.
fn rpc_headers(config: &SupabaseRpcConfig) -> Result<reqwest::header::HeaderMap, DactylError> {
    use reqwest::header::{HeaderMap, HeaderValue};
    let mut headers = HeaderMap::new();
    let mut key = HeaderValue::from_str(config.publishable_key())
        .map_err(|_| DactylError::Config("invalid publishable key".into()))?;
    key.set_sensitive(true);
    let schema = HeaderValue::from_str(config.schema())
        .map_err(|_| DactylError::Config("invalid API schema".into()))?;
    headers.insert("apikey", key);
    headers.insert("content-profile", schema.clone());
    headers.insert("accept-profile", schema);
    headers.insert("accept", HeaderValue::from_static("application/json"));
    headers.insert("dactyl-contract-version", HeaderValue::from_static("1"));
    Ok(headers)
}

struct PreparedControlRequest {
    expected_origin: reqwest::Url,
    endpoint: String,
    canonical: String,
    auth_header: reqwest::header::HeaderValue,
    dpop_header: Option<reqwest::header::HeaderValue>,
    body: Vec<u8>,
    headers: Option<reqwest::header::HeaderMap>,
    budget: ResourceBudget,
    mutates: bool,
    kind: OperationKind,
    attempt: Option<OperationAttempt>,
    resolved_digest: Option<String>,
    attempt_headers: Vec<(&'static str, reqwest::header::HeaderValue)>,
    observation_source: ObservationSource,
    client: Arc<SharedHttpClient>,
}

#[allow(clippy::too_many_arguments)]
fn prepare_control_request(
    config: &SupabaseRpcConfig,
    gateway_prefix: &str,
    function: &str,
    request: &serde_json::Value,
    bearer: Option<&str>,
    dpop_nonce: Option<&str>,
    kind: OperationKind,
    budget: ResourceBudget,
    proof_signer: Option<&Arc<dyn RequestProofSigner>>,
    http_client: Option<Arc<SharedHttpClient>>,
    attempt: Option<&OperationAttempt>,
    observation_source: ObservationSource,
    metadata_out: &mut CloudResponseMetadata,
) -> Result<PreparedControlRequest, DactylError> {
    let record_preflight = |err: DactylError,
                            metadata_out: &mut CloudResponseMetadata,
                            auth_scheme: Option<AuthScheme>| {
        metadata_out.set_provenance(EffectProvenance::local_preflight(
            Some(kind),
            attempt,
            err.adapter_code(),
            auth_scheme,
        ));
        err
    };

    let origin = match validate_rpc(config) {
        Ok(o) => o,
        Err(err) => return Err(record_preflight(err, metadata_out, None)),
    };
    if let Err(err) = budget.validate() {
        return Err(record_preflight(err, metadata_out, None));
    }
    let name = function.as_bytes();
    if name.is_empty()
        || name.len() > 63
        || !(name[0].is_ascii_alphabetic() || name[0] == b'_')
        || !name.iter().all(|b| b.is_ascii_alphanumeric() || *b == b'_')
    {
        return Err(record_preflight(
            DactylError::Config("control RPC requires a function identifier".into()),
            metadata_out,
            None,
        ));
    }
    let token = match bearer {
        Some(t) => t,
        None => {
            return Err(record_preflight(
                DactylError::adapter_with_code(
                    AdapterErrorKind::Authentication,
                    "authentication_required",
                    "local preflight: control RPC requires a user bearer token",
                ),
                metadata_out,
                None,
            ))
        }
    };
    if let Err(err) = crate::connector::validate_bearer_token(token) {
        return Err(record_preflight(err, metadata_out, None));
    }
    if let Some(nonce) = dpop_nonce {
        if let Err(err) = validate_dpop_nonce(nonce) {
            return Err(record_preflight(err, metadata_out, None));
        }
    }
    if ensure_json_depth(request).is_err() {
        return Err(record_preflight(
            preflight_limit(
                "payload_too_deep",
                "control request exceeds JSON nesting limit",
            ),
            metadata_out,
            None,
        ));
    }
    let (body, endpoint, require_dpop_proof) = if config.uses_device_gateway() {
        #[derive(Serialize)]
        struct GatewayControlRequest<'a> {
            version: u16,
            function: &'a str,
            request: &'a serde_json::Value,
        }
        let serialized = match serialize_bounded_json(
            &GatewayControlRequest {
                version: SUPABASE_RPC_CONTRACT_VERSION,
                function,
                request,
            },
            budget.max_wire_bytes(),
        ) {
            Ok(b) => b,
            Err(err) => return Err(record_preflight(err, metadata_out, None)),
        };
        (
            serialized,
            format!(
                "{}{gateway_prefix}/cloud-control",
                origin.as_str().trim_end_matches('/')
            ),
            true,
        )
    } else {
        #[derive(Serialize)]
        struct ControlRequest<'a> {
            request: &'a serde_json::Value,
        }
        let serialized =
            match serialize_bounded_json(&ControlRequest { request }, budget.max_wire_bytes()) {
                Ok(b) => b,
                Err(err) => return Err(record_preflight(err, metadata_out, None)),
            };
        (
            serialized,
            format!(
                "{}/rest/v1/rpc/{function}",
                origin.as_str().trim_end_matches('/')
            ),
            false,
        )
    };
    let headers = match rpc_headers(config) {
        Ok(h) => Some(h),
        Err(err) => return Err(record_preflight(err, metadata_out, None)),
    };
    let canonical = match canonical_url(&endpoint) {
        Ok(c) => c,
        Err(err) => return Err(record_preflight(err, metadata_out, None)),
    };
    let (auth_header, dpop_header) =
        match build_auth_headers("POST", &canonical, token, proof_signer, dpop_nonce) {
            Ok(pair) => pair,
            Err(err) => return Err(record_preflight(err, metadata_out, None)),
        };
    if require_dpop_proof && dpop_header.is_none() {
        return Err(record_preflight(
            DactylError::adapter_with_code(
                AdapterErrorKind::Authentication,
                "device_proof_required",
                "local preflight: control gateway requires a DPoP proof signer",
            ),
            metadata_out,
            Some(AuthScheme::Bearer),
        ));
    }
    let auth_scheme = if dpop_header.is_some() {
        Some(AuthScheme::DPoP)
    } else {
        Some(AuthScheme::Bearer)
    };
    let body_for_digest = if observation_source == ObservationSource::ServerReadback {
        None
    } else {
        Some(body.as_slice())
    };
    let (resolved_digest, attempt_headers) =
        match build_attempt_headers(attempt, body_for_digest, Some(kind)) {
            Ok(pair) => pair,
            Err(err) => return Err(record_preflight(err, metadata_out, auth_scheme)),
        };
    let client = match http_client {
        Some(client) if client.matches(budget) => client,
        _ => match acquire_client(budget) {
            Ok(c) => c,
            Err(err) => return Err(record_preflight(err, metadata_out, auth_scheme)),
        },
    };
    let mutates = kind == OperationKind::Write;
    Ok(PreparedControlRequest {
        expected_origin: origin,
        endpoint,
        canonical,
        auth_header,
        dpop_header,
        body,
        headers,
        budget,
        mutates,
        kind,
        attempt: attempt.cloned(),
        resolved_digest,
        attempt_headers,
        observation_source,
        client,
    })
}

fn finalize_control_outcome(
    outcome: Result<serde_json::Value, DactylError>,
) -> Result<serde_json::Value, DactylError> {
    outcome.map_err(|error| {
        if error.mutation_outcome() == crate::MutationOutcome::Uncertain {
            // Preserve the real transport category while exposing the
            // stable no-replay code used by managed session consumers.
            match error {
                DactylError::Adapter { kind, message, .. } => {
                    DactylError::adapter_with_code(kind, "outcome_unknown", message)
                }
                other => other,
            }
        } else {
            error
        }
    })
}

/// The application supplies a trusted function identifier, never a URL or SQL.
/// Policy and the JSON argument/result remain opaque to the driver.
#[allow(clippy::too_many_arguments)]
pub(crate) fn control_request_with_auth_and_attempt(
    config: &SupabaseRpcConfig,
    gateway_prefix: &str,
    function: &str,
    request: &serde_json::Value,
    bearer: Option<&str>,
    dpop_nonce: Option<&str>,
    kind: OperationKind,
    budget: ResourceBudget,
    proof_signer: Option<Arc<dyn RequestProofSigner>>,
    http_client: Option<Arc<SharedHttpClient>>,
    attempt: Option<&OperationAttempt>,
    observation_source: ObservationSource,
    metadata_out: &mut CloudResponseMetadata,
) -> Result<serde_json::Value, DactylError> {
    let mut prepared = prepare_control_request(
        config,
        gateway_prefix,
        function,
        request,
        bearer,
        dpop_nonce,
        kind,
        budget,
        proof_signer.as_ref(),
        http_client,
        attempt,
        observation_source,
        metadata_out,
    )?;
    let mutates = prepared.mutates;
    let execute = move || {
        let mut req_ctx = RequestExecutionContext {
            expected_origin: prepared.expected_origin,
            endpoint: prepared.endpoint,
            canonical: prepared.canonical,
            auth_header: prepared.auth_header,
            dpop_header: prepared.dpop_header,
            body: prepared.body,
            rpc_headers: prepared.headers,
            budget: prepared.budget,
            mutates: prepared.mutates,
            validate_rows: false,
            operation_kind: Some(prepared.kind),
            attempt: prepared.attempt.take(),
            resolved_digest: prepared.resolved_digest.take(),
            attempt_headers: std::mem::take(&mut prepared.attempt_headers),
            observation_source: prepared.observation_source,
            proof_signer,
        };
        send_request(&prepared.client, &mut req_ctx)
    };
    let (outcome, captured_meta) = if in_tokio_runtime() {
        std::thread::Builder::new()
            .name("dactyl-control-rpc".into())
            .spawn(execute)
            .map_err(|_| transport_error(false))?
            .join()
            .map_err(|_| maybe_uncertain(transport_error(false), mutates))?
    } else {
        execute()
    };
    *metadata_out = captured_meta.unwrap_or_default();
    finalize_control_outcome(outcome)
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn control_request_with_auth_and_attempt_async(
    config: &SupabaseRpcConfig,
    gateway_prefix: &str,
    function: &str,
    request: &serde_json::Value,
    bearer: Option<&str>,
    dpop_nonce: Option<&str>,
    kind: OperationKind,
    budget: ResourceBudget,
    proof_signer: Option<Arc<dyn RequestProofSigner>>,
    http_client: Option<Arc<SharedHttpClient>>,
    attempt: Option<&OperationAttempt>,
    observation_source: ObservationSource,
    metadata_out: &mut CloudResponseMetadata,
) -> Result<serde_json::Value, DactylError> {
    let mut prepared = prepare_control_request(
        config,
        gateway_prefix,
        function,
        request,
        bearer,
        dpop_nonce,
        kind,
        budget,
        proof_signer.as_ref(),
        http_client,
        attempt,
        observation_source,
        metadata_out,
    )?;
    let mut req_ctx = RequestExecutionContext {
        expected_origin: prepared.expected_origin,
        endpoint: prepared.endpoint,
        canonical: prepared.canonical,
        auth_header: prepared.auth_header,
        dpop_header: prepared.dpop_header,
        body: prepared.body,
        rpc_headers: prepared.headers,
        budget: prepared.budget,
        mutates: prepared.mutates,
        validate_rows: false,
        operation_kind: Some(prepared.kind),
        attempt: prepared.attempt.take(),
        resolved_digest: prepared.resolved_digest.take(),
        attempt_headers: std::mem::take(&mut prepared.attempt_headers),
        observation_source: prepared.observation_source,
        proof_signer,
    };
    let (outcome, captured_meta) = send_request_async(&prepared.client, &mut req_ctx).await;
    *metadata_out = captured_meta.unwrap_or_default();
    finalize_control_outcome(outcome)
}

#[derive(Serialize)]
struct RpcRequest<'a, T> {
    request: VersionedRequest<'a, T>,
}
#[derive(Serialize)]
struct VersionedRequest<'a, T> {
    version: u16,
    #[serde(flatten)]
    body: &'a T,
}

impl SupabaseAdapter {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        endpoint: &str,
        bearer: Option<String>,
        options: OpenOptions,
        context: Option<StorageContext>,
        rpc: Option<&SupabaseRpcConfig>,
        budget: ResourceBudget,
        proof_signer: Option<Arc<dyn RequestProofSigner>>,
        dpop_nonce: Option<&str>,
    ) -> Result<Self, DactylError> {
        budget.validate()?;
        let url = match rpc {
            Some(config) => validate_rpc(config)?,
            None => validate_endpoint(endpoint)?,
        };
        let token = bearer
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| {
                DactylError::adapter_with_code(
                    AdapterErrorKind::Authentication,
                    "authentication_required",
                    "Supabase service requires a user bearer token",
                )
            })?;
        let header_check = reqwest::header::HeaderValue::from_str(&format!("Bearer {token}"))
            .map_err(|_| DactylError::Config("invalid Supabase bearer token".into()))?;
        drop(header_check);
        if let Some(nonce) = dpop_nonce {
            validate_dpop_nonce(nonce)?;
        }
        let context = context.ok_or_else(|| {
            DactylError::adapter_with_code(
                AdapterErrorKind::Authentication,
                "missing_context",
                "Supabase service requires an authenticated storage context",
            )
        })?;
        context.validate()?;
        let device_gateway = rpc.is_some_and(SupabaseRpcConfig::uses_device_gateway);
        let rpc_headers = rpc.map(rpc_headers).transpose()?;
        let http_client = acquire_client(budget)?;
        Ok(Self {
            rpc_headers,
            device_gateway,
            endpoint: url.as_str().trim_end_matches('/').to_owned(),
            expected_origin: url,
            token,
            access_mode: options.access_mode,
            context,
            budget,
            proof_signer,
            dpop_nonce: dpop_nonce.map(str::to_owned),
            http_client,
        })
    }

    fn default_auth_scheme(&self) -> Option<AuthScheme> {
        if self.proof_signer.is_some() {
            Some(AuthScheme::DPoP)
        } else {
            Some(AuthScheme::Bearer)
        }
    }

    fn record_preflight(
        &self,
        err: DactylError,
        kind: OperationKind,
        attempt: Option<&OperationAttempt>,
        metadata_out: &mut CloudResponseMetadata,
    ) -> DactylError {
        metadata_out.set_provenance(EffectProvenance::local_preflight(
            Some(kind),
            attempt,
            err.adapter_code(),
            self.default_auth_scheme(),
        ));
        err
    }

    #[allow(clippy::too_many_arguments)]
    fn prepare_send(
        &self,
        path: &str,
        request: &impl Serialize,
        mutates: bool,
        validate_rows: bool,
        operation_kind: OperationKind,
        attempt: Option<&OperationAttempt>,
        metadata_out: &mut CloudResponseMetadata,
    ) -> Result<RequestExecutionContext, DactylError> {
        let max_wire_bytes = self.budget.max_wire_bytes();
        let body = if self.device_gateway {
            serialize_bounded_json(
                &VersionedRequest {
                    version: SUPABASE_RPC_CONTRACT_VERSION,
                    body: request,
                },
                max_wire_bytes,
            )
        } else if self.rpc_headers.is_some() {
            serialize_bounded_json(
                &RpcRequest {
                    request: VersionedRequest {
                        version: SUPABASE_RPC_CONTRACT_VERSION,
                        body: request,
                    },
                },
                max_wire_bytes,
            )
        } else {
            serialize_bounded_json(request, max_wire_bytes)
        }
        .map_err(|err| self.record_preflight(err, operation_kind, attempt, metadata_out))?;
        let path = if self.device_gateway {
            match path {
                "/query" => "/functions/v1/decapod-onboarding/cloud-query",
                "/batch" => "/functions/v1/decapod-onboarding/cloud-batch",
                _ => {
                    return Err(self.record_preflight(
                        protocol_error("unsupported RPC operation"),
                        operation_kind,
                        attempt,
                        metadata_out,
                    ))
                }
            }
        } else if self.rpc_headers.is_some() {
            match path {
                "/query" => "/rest/v1/rpc/dactyl_query_v1",
                "/batch" if self.access_mode == AccessMode::ReadOnly => {
                    "/rest/v1/rpc/dactyl_read_batch_v1"
                }
                "/batch" => "/rest/v1/rpc/dactyl_batch_v1",
                _ => {
                    return Err(self.record_preflight(
                        protocol_error("unsupported RPC operation"),
                        operation_kind,
                        attempt,
                        metadata_out,
                    ))
                }
            }
        } else {
            path
        };
        let endpoint = format!("{}{path}", self.endpoint);
        let canonical = canonical_url(&endpoint)
            .map_err(|err| self.record_preflight(err, operation_kind, attempt, metadata_out))?;
        let (auth_header, dpop_header) = build_auth_headers(
            "POST",
            &canonical,
            &self.token,
            self.proof_signer.as_ref(),
            self.dpop_nonce.as_deref(),
        )
        .map_err(|err| self.record_preflight(err, operation_kind, attempt, metadata_out))?;
        if self.device_gateway && dpop_header.is_none() {
            return Err(self.record_preflight(
                DactylError::adapter_with_code(
                    AdapterErrorKind::Authentication,
                    "device_proof_required",
                    "local preflight: device gateway requests require a DPoP proof signer",
                ),
                operation_kind,
                attempt,
                metadata_out,
            ));
        }
        let (resolved_digest, attempt_headers) =
            build_attempt_headers(attempt, Some(body.as_slice()), Some(operation_kind))
                .map_err(|err| self.record_preflight(err, operation_kind, attempt, metadata_out))?;
        Ok(RequestExecutionContext {
            expected_origin: self.expected_origin.clone(),
            endpoint,
            canonical,
            auth_header,
            dpop_header,
            body,
            rpc_headers: self.rpc_headers.clone(),
            budget: self.budget,
            mutates,
            validate_rows,
            operation_kind: Some(operation_kind),
            attempt: attempt.cloned(),
            resolved_digest,
            attempt_headers,
            observation_source: ObservationSource::DirectResponse,
            proof_signer: self.proof_signer.clone(),
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn send_with_attempt<T: for<'de> Deserialize<'de> + Send + 'static>(
        &self,
        path: &str,
        request: &impl Serialize,
        mutates: bool,
        operation_kind: OperationKind,
        attempt: Option<&OperationAttempt>,
        metadata_out: &mut CloudResponseMetadata,
    ) -> Result<T, DactylError> {
        let mut req_ctx = self.prepare_send(
            path,
            request,
            mutates,
            true,
            operation_kind,
            attempt,
            metadata_out,
        )?;
        let client = self.http_client.clone();
        let (outcome, captured_meta) = if in_tokio_runtime() {
            std::thread::Builder::new()
                .name("dactyl-supabase-http".into())
                .spawn(move || send_request(&client, &mut req_ctx))
                .map_err(|_| transport_error(false))?
                .join()
                .map_err(|_| maybe_uncertain(transport_error(false), mutates))?
        } else {
            send_request(&client, &mut req_ctx)
        };
        *metadata_out = captured_meta.unwrap_or_default();
        outcome
    }

    #[allow(clippy::too_many_arguments)]
    async fn send_with_attempt_async<T: for<'de> Deserialize<'de> + Send + 'static>(
        &self,
        path: &str,
        request: &impl Serialize,
        mutates: bool,
        operation_kind: OperationKind,
        attempt: Option<&OperationAttempt>,
        metadata_out: &mut CloudResponseMetadata,
    ) -> Result<T, DactylError> {
        let mut req_ctx = self.prepare_send(
            path,
            request,
            mutates,
            true,
            operation_kind,
            attempt,
            metadata_out,
        )?;
        let (outcome, captured_meta) = send_request_async(&self.http_client, &mut req_ctx).await;
        *metadata_out = captured_meta.unwrap_or_default();
        outcome
    }

    fn prepare_query_preflight(
        &self,
        sql: &str,
        params: &[Parameter],
        kind: OperationKind,
        attempt: Option<&OperationAttempt>,
        metadata_out: &mut CloudResponseMetadata,
    ) -> Result<(), DactylError> {
        if let Err(err) = check_parameters(params) {
            return Err(self.record_preflight(err, kind, attempt, metadata_out));
        }
        if let Err(err) = preflight_statement(&self.budget, sql, params) {
            return Err(self.record_preflight(err, kind, attempt, metadata_out));
        }
        Ok(())
    }

    fn request_with_attempt(
        &self,
        sql: &str,
        params: &[Parameter],
        kind: OperationKind,
        mutates: bool,
        attempt: Option<&OperationAttempt>,
        metadata_out: &mut CloudResponseMetadata,
    ) -> Result<Response, DactylError> {
        self.prepare_query_preflight(sql, params, kind, attempt, metadata_out)?;
        self.send_with_attempt(
            "/query",
            &Request {
                sql,
                params,
                kind,
                // Read methods cannot be used as a write escape hatch even on a
                // read-write connection. The service must enforce this physically.
                access_mode: if kind == OperationKind::Read {
                    AccessMode::ReadOnly
                } else {
                    self.access_mode
                },
                context: &self.context,
            },
            mutates,
            kind,
            attempt,
            metadata_out,
        )
    }

    async fn request_with_attempt_async(
        &self,
        sql: &str,
        params: &[Parameter],
        kind: OperationKind,
        mutates: bool,
        attempt: Option<&OperationAttempt>,
        metadata_out: &mut CloudResponseMetadata,
    ) -> Result<Response, DactylError> {
        self.prepare_query_preflight(sql, params, kind, attempt, metadata_out)?;
        self.send_with_attempt_async(
            "/query",
            &Request {
                sql,
                params,
                kind,
                access_mode: if kind == OperationKind::Read {
                    AccessMode::ReadOnly
                } else {
                    self.access_mode
                },
                context: &self.context,
            },
            mutates,
            kind,
            attempt,
            metadata_out,
        )
        .await
    }

    fn request(
        &self,
        sql: &str,
        params: &[Parameter],
        kind: OperationKind,
        mutates: bool,
    ) -> Result<Response, DactylError> {
        let mut metadata = CloudResponseMetadata::default();
        self.request_with_attempt(sql, params, kind, mutates, None, &mut metadata)
    }

    async fn request_async(
        &self,
        sql: &str,
        params: &[Parameter],
        kind: OperationKind,
        mutates: bool,
    ) -> Result<Response, DactylError> {
        let mut metadata = CloudResponseMetadata::default();
        self.request_with_attempt_async(sql, params, kind, mutates, None, &mut metadata)
            .await
    }
}

fn maybe_uncertain(error: DactylError, mutates: bool) -> DactylError {
    if mutates {
        mark_remote_uncertain(error)
    } else {
        error
    }
}

struct RequestExecutionContext {
    expected_origin: reqwest::Url,
    endpoint: String,
    canonical: String,
    auth_header: reqwest::header::HeaderValue,
    dpop_header: Option<reqwest::header::HeaderValue>,
    body: Vec<u8>,
    rpc_headers: Option<reqwest::header::HeaderMap>,
    budget: ResourceBudget,
    mutates: bool,
    validate_rows: bool,
    operation_kind: Option<OperationKind>,
    attempt: Option<OperationAttempt>,
    resolved_digest: Option<String>,
    attempt_headers: Vec<(&'static str, reqwest::header::HeaderValue)>,
    observation_source: ObservationSource,
    proof_signer: Option<Arc<dyn RequestProofSigner>>,
}

fn fault_metadata_from_ctx(
    ctx: &RequestExecutionContext,
    auth: &AuthHeaders,
    mut metadata: CloudResponseMetadata,
    reason_code: &'static str,
    origin_bound: bool,
) -> CloudResponseMetadata {
    metadata.set_provenance(EffectProvenance::transport_fault(
        ctx.operation_kind,
        ctx.attempt.as_ref(),
        ctx.resolved_digest.as_deref(),
        metadata
            .cloud_header("decapod-cloud-request-id")
            .map(str::to_owned),
        Some(reason_code),
        effective_auth_scheme(auth),
        origin_bound,
        ctx.expected_origin.scheme() == "https",
    ));
    metadata
}

fn pre_send_timeout_outcome<T>(
    ctx: &RequestExecutionContext,
    auth: &AuthHeaders,
    code: &'static str,
    message: &'static str,
) -> HttpOutcome<T> {
    let mut metadata = CloudResponseMetadata::default();
    metadata.set_provenance(EffectProvenance::local_preflight(
        ctx.operation_kind,
        ctx.attempt.as_ref(),
        Some(code),
        effective_auth_scheme(auth),
    ));
    (
        Err(DactylError::adapter_with_code(
            AdapterErrorKind::Timeout,
            code,
            message,
        )),
        Some(metadata),
    )
}

#[allow(clippy::too_many_arguments)]
fn handle_supabase_response<T: for<'de> Deserialize<'de>>(
    ctx: &RequestExecutionContext,
    auth: &AuthHeaders,
    status: u16,
    response_headers: &reqwest::header::HeaderMap,
    response_url: &reqwest::Url,
    bytes: &[u8],
    mut metadata: CloudResponseMetadata,
    dpop_challenge: bool,
) -> HttpOutcome<T> {
    let rpc = ctx.rpc_headers.is_some();
    let effective_auth = effective_auth_scheme(auth);
    let tls_authenticated = ctx.expected_origin.scheme() == "https";
    let origin_matches = response_url.origin() == ctx.expected_origin.origin();
    let prov_ctx = ProvenanceCheckContext {
        expected_origin: &ctx.expected_origin,
        response_url,
        expected_auth: effective_auth,
        expected_kind: ctx.operation_kind,
        attempt: ctx.attempt.as_ref(),
        resolved_digest: ctx.resolved_digest.as_deref(),
        observation_source: ctx.observation_source,
        http_status: status,
        is_mutating: ctx.mutates,
    };

    if !(200..300).contains(&status) {
        if (300..400).contains(&status) {
            metadata =
                fault_metadata_from_ctx(ctx, auth, metadata, "provenance_origin_mismatch", false);
            return (
                Err(maybe_uncertain(
                    DactylError::adapter_with_code(
                        AdapterErrorKind::Protocol,
                        "provenance_origin_mismatch",
                        format!("Supabase service returned HTTP status {status}"),
                    ),
                    ctx.mutates,
                )),
                Some(metadata),
            );
        }

        let envelope = serde_json::from_slice::<serde_json::Value>(bytes)
            .ok()
            .filter(|value| ensure_json_depth(value).is_ok());
        let mut error = remote_error(status, bytes, rpc);
        if dpop_challenge && error.adapter_code().is_none() {
            error = DactylError::adapter_with_code(
                AdapterErrorKind::Authentication,
                "use_dpop_nonce",
                "Supabase service rejected request (use_dpop_nonce)",
            );
        }
        if let Err(prov_err) = bind_response_provenance(
            response_headers,
            envelope.as_ref(),
            &prov_ctx,
            &mut metadata,
            error.adapter_code(),
        ) {
            let err = if matches!(
                prov_err.adapter_kind(),
                Some(AdapterErrorKind::IdempotencyConflict)
            ) {
                prov_err
            } else {
                maybe_uncertain(prov_err, ctx.mutates)
            };
            return (Err(err), Some(metadata));
        }

        if ctx.mutates && matches!(status, 408 | 500..=599) {
            let explicit_non_commit = metadata.provenance().is_some_and(|p| {
                matches!(
                    p.status(),
                    crate::connector::EffectStatus::RolledBack
                        | crate::connector::EffectStatus::NotAttempted
                        | crate::connector::EffectStatus::Refused
                )
            });
            if !explicit_non_commit {
                if matches!(status, 500..=599) && error.adapter_code().is_none() {
                    if let DactylError::Adapter { kind, message, .. } = error {
                        error = DactylError::adapter_with_code(kind, "outcome_unknown", message);
                    }
                }
                return (Err(mark_remote_uncertain(error)), Some(metadata));
            }
        }
        return (Err(error), Some(metadata));
    }

    let value: serde_json::Value = match serde_json::from_slice(bytes) {
        Ok(value) => value,
        Err(_) => {
            metadata =
                fault_metadata_from_ctx(ctx, auth, metadata, "malformed_response", origin_matches);
            return (
                Err(maybe_uncertain(
                    protocol_error(if rpc {
                        "invalid RPC response"
                    } else {
                        "invalid Supabase service response"
                    }),
                    ctx.mutates,
                )),
                Some(metadata),
            );
        }
    };
    if let Err(error) = ensure_json_depth(&value) {
        metadata = fault_metadata_from_ctx(ctx, auth, metadata, "payload_too_deep", origin_matches);
        return (Err(maybe_uncertain(error, ctx.mutates)), Some(metadata));
    }
    if let Err(prov_err) = bind_response_provenance(
        response_headers,
        Some(&value),
        &prov_ctx,
        &mut metadata,
        None,
    ) {
        let err = if matches!(
            prov_err.adapter_kind(),
            Some(
                AdapterErrorKind::Authorization
                    | AdapterErrorKind::TransactionAborted
                    | AdapterErrorKind::IdempotencyConflict
            )
        ) {
            prov_err
        } else {
            maybe_uncertain(prov_err, ctx.mutates)
        };
        return (Err(err), Some(metadata));
    }

    if rpc && ctx.validate_rows {
        if let Some(results) = value.get("results") {
            let mut tracker = DecodedResultTracker::new(ctx.budget);
            let array = match results.as_array() {
                Some(array) => array,
                None => {
                    return (
                        Err(maybe_uncertain(
                            protocol_error("invalid RPC batch response"),
                            ctx.mutates,
                        )),
                        Some(metadata),
                    )
                }
            };
            for result in array {
                if let Err(error) = validate_rpc_result(result, ctx.budget, &mut tracker) {
                    return (Err(maybe_uncertain(error, ctx.mutates)), Some(metadata));
                }
            }
        } else {
            let mut tracker = DecodedResultTracker::new(ctx.budget);
            if let Err(error) = validate_rpc_result(&value, ctx.budget, &mut tracker) {
                return (Err(maybe_uncertain(error, ctx.mutates)), Some(metadata));
            }
        }
        let parsed = serde_json::from_value(value)
            .map_err(|_| maybe_uncertain(protocol_error("invalid RPC response"), ctx.mutates));
        return (parsed, Some(metadata));
    }

    let parsed = serde_json::from_value(value).map_err(|_| {
        maybe_uncertain(
            protocol_error("invalid Supabase service response"),
            ctx.mutates,
        )
    });
    if parsed.is_err()
        && ctx.mutates
        && metadata
            .provenance()
            .is_none_or(|p| p.status() == crate::connector::EffectStatus::UnsupportedProvenance)
    {
        metadata.set_provenance(EffectProvenance::transport_fault(
            ctx.operation_kind,
            ctx.attempt.as_ref(),
            ctx.resolved_digest.as_deref(),
            metadata
                .cloud_header("decapod-cloud-request-id")
                .map(str::to_owned),
            Some("malformed_response"),
            effective_auth,
            origin_matches,
            tls_authenticated,
        ));
    }
    (parsed, Some(metadata))
}

fn send_once<T: for<'de> Deserialize<'de>>(
    blocking_client: &reqwest::blocking::Client,
    ctx: &RequestExecutionContext,
    auth: &AuthHeaders,
    deadline: Instant,
    unauthorized: &mut bool,
) -> HttpOutcome<T> {
    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        return pre_send_timeout_outcome(
            ctx,
            auth,
            "timeout",
            "local preflight: request deadline expired before dispatch",
        );
    }
    let mut request = blocking_client
        .post(&ctx.endpoint)
        .timeout(remaining)
        .headers(ctx.rpc_headers.clone().unwrap_or_default())
        .header(reqwest::header::CONTENT_TYPE, "application/json");
    if let Some(auth_header) = auth.0.as_ref() {
        request = request.header(reqwest::header::AUTHORIZATION, auth_header.clone());
    }
    if let Some(dpop) = auth.1.as_ref() {
        request = request.header("DPoP", dpop.clone());
    }
    for (name, value) in &ctx.attempt_headers {
        request = request.header(*name, value.clone());
    }
    let response = match request.body(ctx.body.clone()).send() {
        Ok(response) => response,
        Err(error) => {
            let meta = fault_metadata_from_ctx(
                ctx,
                auth,
                CloudResponseMetadata::default(),
                "outcome_unknown",
                false,
            );
            return (
                Err(maybe_uncertain(
                    transport_error(error.is_timeout()),
                    ctx.mutates,
                )),
                Some(meta),
            );
        }
    };
    let status = response.status().as_u16();
    *unauthorized = status == 401;
    let response_url = response.url().clone();
    let response_headers = response.headers().clone();
    let origin_matches = response_url.origin() == ctx.expected_origin.origin();
    let dpop_challenge = status == 401 && has_dpop_nonce_challenge(&response_headers);
    let metadata =
        extract_response_metadata(&response_headers, &ctx.canonical, ctx.proof_signer.as_ref());
    let bytes = match read_bounded_body(response, ctx.budget.max_wire_bytes()) {
        Ok(bytes) => bytes,
        Err(BoundedReadError::TooLarge) => {
            let meta =
                fault_metadata_from_ctx(ctx, auth, metadata, "response_too_large", origin_matches);
            return (
                Err(maybe_uncertain(
                    DactylError::adapter_with_code(
                        AdapterErrorKind::Protocol,
                        "response_too_large",
                        "Supabase response exceeds max_wire_bytes",
                    ),
                    ctx.mutates,
                )),
                Some(meta),
            );
        }
        Err(BoundedReadError::Io(error)) => {
            let meta =
                fault_metadata_from_ctx(ctx, auth, metadata, "outcome_unknown", origin_matches);
            return (
                Err(maybe_uncertain(
                    transport_error(body_read_timed_out(&error)),
                    ctx.mutates,
                )),
                Some(meta),
            );
        }
    };
    handle_supabase_response(
        ctx,
        auth,
        status,
        &response_headers,
        &response_url,
        &bytes,
        metadata,
        dpop_challenge,
    )
}

async fn send_once_async<T: for<'de> Deserialize<'de>>(
    async_client: &reqwest::Client,
    ctx: &RequestExecutionContext,
    auth: &AuthHeaders,
    deadline: Instant,
) -> (HttpOutcome<T>, bool) {
    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        return (
            pre_send_timeout_outcome(
                ctx,
                auth,
                "timeout",
                "local preflight: request deadline expired before dispatch",
            ),
            false,
        );
    }
    let mut request = async_client
        .post(&ctx.endpoint)
        .timeout(remaining)
        .headers(ctx.rpc_headers.clone().unwrap_or_default())
        .header(reqwest::header::CONTENT_TYPE, "application/json");
    if let Some(auth_header) = auth.0.as_ref() {
        request = request.header(reqwest::header::AUTHORIZATION, auth_header.clone());
    }
    if let Some(dpop) = auth.1.as_ref() {
        request = request.header("DPoP", dpop.clone());
    }
    for (name, value) in &ctx.attempt_headers {
        request = request.header(*name, value.clone());
    }
    let response =
        match tokio::time::timeout(remaining, request.body(ctx.body.clone()).send()).await {
            Ok(Ok(response)) => response,
            Ok(Err(error)) => {
                let meta = fault_metadata_from_ctx(
                    ctx,
                    auth,
                    CloudResponseMetadata::default(),
                    "outcome_unknown",
                    false,
                );
                return (
                    (
                        Err(maybe_uncertain(
                            transport_error(error.is_timeout()),
                            ctx.mutates,
                        )),
                        Some(meta),
                    ),
                    false,
                );
            }
            Err(_) => {
                let meta = fault_metadata_from_ctx(
                    ctx,
                    auth,
                    CloudResponseMetadata::default(),
                    "outcome_unknown",
                    false,
                );
                return (
                    (
                        Err(maybe_uncertain(transport_error(true), ctx.mutates)),
                        Some(meta),
                    ),
                    false,
                );
            }
        };
    let status = response.status().as_u16();
    let unauthorized = status == 401;
    let response_url = response.url().clone();
    let response_headers = response.headers().clone();
    let origin_matches = response_url.origin() == ctx.expected_origin.origin();
    let dpop_challenge = unauthorized && has_dpop_nonce_challenge(&response_headers);
    let metadata =
        extract_response_metadata(&response_headers, &ctx.canonical, ctx.proof_signer.as_ref());
    let body_remaining = deadline.saturating_duration_since(Instant::now());
    let bytes = match tokio::time::timeout(
        body_remaining,
        read_bounded_body_async(response, ctx.budget.max_wire_bytes()),
    )
    .await
    {
        Ok(Ok(bytes)) => bytes,
        Ok(Err(AsyncBodyError::TooLarge)) => {
            let meta =
                fault_metadata_from_ctx(ctx, auth, metadata, "response_too_large", origin_matches);
            return (
                (
                    Err(maybe_uncertain(
                        DactylError::adapter_with_code(
                            AdapterErrorKind::Protocol,
                            "response_too_large",
                            "Supabase response exceeds max_wire_bytes",
                        ),
                        ctx.mutates,
                    )),
                    Some(meta),
                ),
                unauthorized,
            );
        }
        Ok(Err(AsyncBodyError::Transport(error))) => {
            let meta =
                fault_metadata_from_ctx(ctx, auth, metadata, "outcome_unknown", origin_matches);
            return (
                (
                    Err(maybe_uncertain(
                        transport_error(error.is_timeout()),
                        ctx.mutates,
                    )),
                    Some(meta),
                ),
                unauthorized,
            );
        }
        Err(_) => {
            let meta =
                fault_metadata_from_ctx(ctx, auth, metadata, "outcome_unknown", origin_matches);
            return (
                (
                    Err(maybe_uncertain(transport_error(true), ctx.mutates)),
                    Some(meta),
                ),
                unauthorized,
            );
        }
    };
    (
        handle_supabase_response(
            ctx,
            auth,
            status,
            &response_headers,
            &response_url,
            &bytes,
            metadata,
            dpop_challenge,
        ),
        unauthorized,
    )
}

fn send_request<T: for<'de> Deserialize<'de>>(
    http_client: &SharedHttpClient,
    ctx: &mut RequestExecutionContext,
) -> HttpOutcome<T> {
    let blocking_client = match http_client.blocking_client() {
        Ok(client) => client,
        Err(err) => {
            let mut meta = CloudResponseMetadata::default();
            meta.set_provenance(EffectProvenance::local_preflight(
                ctx.operation_kind,
                ctx.attempt.as_ref(),
                err.adapter_code(),
                effective_auth_scheme(&(Some(ctx.auth_header.clone()), ctx.dpop_header.clone())),
            ));
            return (Err(err), Some(meta));
        }
    };
    let deadline = Instant::now() + ctx.budget.request_timeout();
    let _permit = match http_client.acquire_permit(deadline) {
        Ok(permit) => permit,
        Err(err) => {
            let auth = (Some(ctx.auth_header.clone()), ctx.dpop_header.clone());
            let mut meta = CloudResponseMetadata::default();
            meta.set_provenance(EffectProvenance::local_preflight(
                ctx.operation_kind,
                ctx.attempt.as_ref(),
                err.adapter_code(),
                effective_auth_scheme(&auth),
            ));
            return (Err(err), Some(meta));
        }
    };
    let auth = (Some(ctx.auth_header.clone()), ctx.dpop_header.clone());
    let (outcome, _) = with_dpop_nonce_retry(
        "POST",
        &ctx.canonical,
        ctx.proof_signer.as_ref(),
        auth,
        false,
        |auth, unauthorized| send_once(&blocking_client, ctx, auth, deadline, unauthorized),
    );
    outcome
}

async fn send_request_async<T: for<'de> Deserialize<'de>>(
    http_client: &SharedHttpClient,
    ctx: &mut RequestExecutionContext,
) -> HttpOutcome<T> {
    let async_client = match http_client.async_client() {
        Ok(client) => client,
        Err(err) => {
            let mut meta = CloudResponseMetadata::default();
            meta.set_provenance(EffectProvenance::local_preflight(
                ctx.operation_kind,
                ctx.attempt.as_ref(),
                err.adapter_code(),
                effective_auth_scheme(&(Some(ctx.auth_header.clone()), ctx.dpop_header.clone())),
            ));
            return (Err(err), Some(meta));
        }
    };
    let deadline = Instant::now() + ctx.budget.request_timeout();
    let _permit = match http_client.acquire_permit_async(deadline).await {
        Ok(permit) => permit,
        Err(err) => {
            let auth = (Some(ctx.auth_header.clone()), ctx.dpop_header.clone());
            let mut meta = CloudResponseMetadata::default();
            meta.set_provenance(EffectProvenance::local_preflight(
                ctx.operation_kind,
                ctx.attempt.as_ref(),
                err.adapter_code(),
                effective_auth_scheme(&auth),
            ));
            return (Err(err), Some(meta));
        }
    };
    let auth = (Some(ctx.auth_header.clone()), ctx.dpop_header.clone());
    let (outcome, _) = with_dpop_nonce_retry_async(
        "POST",
        &ctx.canonical,
        ctx.proof_signer.as_ref(),
        auth,
        false,
        |auth| {
            let client = &async_client;
            let ctx_ref = &*ctx;
            async move { send_once_async(client, ctx_ref, &auth, deadline).await }
        },
    )
    .await;
    outcome
}

// Strict native results include keys and validate auxiliary write rows too.
// A client decoding failure does not establish rollback of a committed write.
fn validate_rpc_result(
    value: &serde_json::Value,
    budget: ResourceBudget,
    tracker: &mut DecodedResultTracker,
) -> Result<(), DactylError> {
    if value.get("generated_keys").is_none() {
        return Err(protocol_error("RPC result requires generated_keys"));
    }
    let result: Response =
        serde_json::from_value(value.clone()).map_err(|_| protocol_error("invalid RPC result"))?;
    rows_from_response_budgeted(result, budget, tracker).map(|_| ())
}

pub(crate) fn body_read_timed_out(error: &std::io::Error) -> bool {
    // reqwest's Read implementation wraps its typed timeout in an Other I/O
    // error. Preserve that identity without inspecting potentially secret text.
    error.kind() == std::io::ErrorKind::TimedOut
        || error
            .get_ref()
            .and_then(|source| source.downcast_ref::<reqwest::Error>())
            .is_some_and(reqwest::Error::is_timeout)
}

impl SupabaseAdapter {
    fn decode_write_response(&self, response: Response) -> Result<WriteResult, DactylError> {
        if !response.rows.is_empty() || !response.columns.is_empty() {
            let mut tracker = DecodedResultTracker::new(self.budget);
            let auxiliary = Response {
                columns: response.columns.clone(),
                rows: response.rows.clone(),
                affected_rows: response.affected_rows,
                generated_keys: Vec::new(),
            };
            rows_from_response_budgeted(auxiliary, self.budget, &mut tracker)
                .map_err(mark_remote_uncertain)?;
        }
        Ok(write_from_response(response))
    }

    fn prepare_atomic_preflight(
        &self,
        operations: &[Operation],
        attempt: Option<&OperationAttempt>,
        metadata_out: &mut CloudResponseMetadata,
    ) -> Result<(bool, OperationKind), DactylError> {
        let mutates = operations
            .iter()
            .any(|operation| operation.kind() != OperationKind::Read);
        let batch_kind = if operations
            .iter()
            .any(|operation| operation.kind() == OperationKind::Schema)
        {
            OperationKind::Schema
        } else if mutates {
            OperationKind::Write
        } else {
            OperationKind::Read
        };
        if mutates {
            if let Err(err) = ensure_writable(self.access_mode) {
                return Err(self.record_preflight(err, batch_kind, attempt, metadata_out));
            }
        }
        for operation in operations {
            if let Err(err) = check_parameters(operation.params()) {
                return Err(self.record_preflight(err, batch_kind, attempt, metadata_out));
            }
        }
        if let Err(err) = preflight_batch(&self.budget, operations) {
            return Err(self.record_preflight(err, batch_kind, attempt, metadata_out));
        }
        Ok((mutates, batch_kind))
    }

    fn decode_atomic_response(
        &self,
        operations: &[Operation],
        response: BatchResponse,
        mutates: bool,
    ) -> Result<AtomicResult, DactylError> {
        if response.results.len() != operations.len() {
            return Err(maybe_uncertain(
                protocol_error("Supabase batch result count does not match operation count"),
                mutates,
            ));
        }
        let mut tracker = DecodedResultTracker::new(self.budget);
        let results = operations
            .iter()
            .zip(response.results)
            .map(|(operation, result)| match operation.kind() {
                OperationKind::Read => {
                    rows_from_response_budgeted(result, self.budget, &mut tracker)
                        .map(OperationResult::Rows)
                        .map_err(|error| maybe_uncertain(error, mutates))
                }
                OperationKind::Write | OperationKind::Schema => {
                    Ok(OperationResult::Write(write_from_response(result)))
                }
            })
            .collect::<Result<_, _>>()?;
        Ok(AtomicResult { results })
    }
}

impl Adapter for SupabaseAdapter {
    fn read(&self, sql: &str, params: &[Parameter]) -> Result<Rows, DactylError> {
        let response = self.request(sql, params, OperationKind::Read, false)?;
        let mut tracker = DecodedResultTracker::new(self.budget);
        rows_from_response_budgeted(response, self.budget, &mut tracker)
    }

    fn read_async<'a>(&'a self, sql: &'a str, params: &'a [Parameter]) -> AdapterFuture<'a, Rows> {
        Box::pin(async move {
            let response = self
                .request_async(sql, params, OperationKind::Read, false)
                .await?;
            let mut tracker = DecodedResultTracker::new(self.budget);
            rows_from_response_budgeted(response, self.budget, &mut tracker)
        })
    }

    fn visit_rows(
        &self,
        sql: &str,
        params: &[Parameter],
        visitor: &mut dyn FnMut(&Row) -> Result<bool, DactylError>,
    ) -> Result<ScanOutcome, DactylError> {
        let response = self.request(sql, params, OperationKind::Read, false)?;
        let mut tracker = DecodedResultTracker::new(self.budget);
        visit_response_rows(response, self.budget, &mut tracker, visitor)
    }

    fn visit_rows_async<'a>(
        &'a self,
        sql: &'a str,
        params: &'a [Parameter],
        visitor: &'a mut (dyn FnMut(&Row) -> Result<bool, DactylError> + Send),
    ) -> AdapterFuture<'a, ScanOutcome> {
        Box::pin(async move {
            let response = self
                .request_async(sql, params, OperationKind::Read, false)
                .await?;
            let mut tracker = DecodedResultTracker::new(self.budget);
            visit_response_rows(response, self.budget, &mut tracker, visitor)
        })
    }

    fn write(&self, sql: &str, params: &[Parameter]) -> Result<WriteResult, DactylError> {
        ensure_writable(self.access_mode)?;
        let response = self.request(sql, params, OperationKind::Write, true)?;
        self.decode_write_response(response)
    }

    fn write_async<'a>(
        &'a self,
        sql: &'a str,
        params: &'a [Parameter],
    ) -> AdapterFuture<'a, WriteResult> {
        Box::pin(async move {
            ensure_writable(self.access_mode)?;
            let response = self
                .request_async(sql, params, OperationKind::Write, true)
                .await?;
            self.decode_write_response(response)
        })
    }

    fn write_with_attempt(
        &self,
        sql: &str,
        params: &[Parameter],
        attempt: &OperationAttempt,
        metadata_out: &mut CloudResponseMetadata,
    ) -> Result<WriteResult, DactylError> {
        if let Err(err) = ensure_writable(self.access_mode) {
            return Err(self.record_preflight(
                err,
                OperationKind::Write,
                Some(attempt),
                metadata_out,
            ));
        }
        let response = self.request_with_attempt(
            sql,
            params,
            OperationKind::Write,
            true,
            Some(attempt),
            metadata_out,
        )?;
        self.decode_write_response(response)
    }

    fn write_with_attempt_async<'a>(
        &'a self,
        sql: &'a str,
        params: &'a [Parameter],
        attempt: &'a OperationAttempt,
        metadata_out: &'a mut CloudResponseMetadata,
    ) -> AdapterFuture<'a, WriteResult> {
        Box::pin(async move {
            if let Err(err) = ensure_writable(self.access_mode) {
                return Err(self.record_preflight(
                    err,
                    OperationKind::Write,
                    Some(attempt),
                    metadata_out,
                ));
            }
            let response = self
                .request_with_attempt_async(
                    sql,
                    params,
                    OperationKind::Write,
                    true,
                    Some(attempt),
                    metadata_out,
                )
                .await?;
            self.decode_write_response(response)
        })
    }

    fn atomic(&self, operations: &[Operation]) -> Result<AtomicResult, DactylError> {
        let mut metadata = CloudResponseMetadata::default();
        if operations.is_empty() {
            return Ok(AtomicResult::default());
        }
        let (mutates, batch_kind) =
            self.prepare_atomic_preflight(operations, None, &mut metadata)?;
        let response: BatchResponse = self.send_with_attempt(
            "/batch",
            &BatchRequest {
                operations,
                access_mode: self.access_mode,
                context: &self.context,
            },
            mutates,
            batch_kind,
            None,
            &mut metadata,
        )?;
        self.decode_atomic_response(operations, response, mutates)
    }

    fn atomic_async<'a>(&'a self, operations: &'a [Operation]) -> AdapterFuture<'a, AtomicResult> {
        Box::pin(async move {
            let mut metadata = CloudResponseMetadata::default();
            if operations.is_empty() {
                return Ok(AtomicResult::default());
            }
            let (mutates, batch_kind) =
                self.prepare_atomic_preflight(operations, None, &mut metadata)?;
            let response: BatchResponse = self
                .send_with_attempt_async(
                    "/batch",
                    &BatchRequest {
                        operations,
                        access_mode: self.access_mode,
                        context: &self.context,
                    },
                    mutates,
                    batch_kind,
                    None,
                    &mut metadata,
                )
                .await?;
            self.decode_atomic_response(operations, response, mutates)
        })
    }

    fn atomic_with_attempt(
        &self,
        operations: &[Operation],
        attempt: &OperationAttempt,
        metadata_out: &mut CloudResponseMetadata,
    ) -> Result<AtomicResult, DactylError> {
        if operations.is_empty() {
            metadata_out.set_provenance(EffectProvenance::local_preflight(
                None,
                Some(attempt),
                None,
                self.default_auth_scheme(),
            ));
            return Ok(AtomicResult::default());
        }
        let (mutates, batch_kind) =
            self.prepare_atomic_preflight(operations, Some(attempt), metadata_out)?;
        let response: BatchResponse = self.send_with_attempt(
            "/batch",
            &BatchRequest {
                operations,
                access_mode: self.access_mode,
                context: &self.context,
            },
            mutates,
            batch_kind,
            Some(attempt),
            metadata_out,
        )?;
        self.decode_atomic_response(operations, response, mutates)
    }

    fn atomic_with_attempt_async<'a>(
        &'a self,
        operations: &'a [Operation],
        attempt: &'a OperationAttempt,
        metadata_out: &'a mut CloudResponseMetadata,
    ) -> AdapterFuture<'a, AtomicResult> {
        Box::pin(async move {
            if operations.is_empty() {
                metadata_out.set_provenance(EffectProvenance::local_preflight(
                    None,
                    Some(attempt),
                    None,
                    self.default_auth_scheme(),
                ));
                return Ok(AtomicResult::default());
            }
            let (mutates, batch_kind) =
                self.prepare_atomic_preflight(operations, Some(attempt), metadata_out)?;
            let response: BatchResponse = self
                .send_with_attempt_async(
                    "/batch",
                    &BatchRequest {
                        operations,
                        access_mode: self.access_mode,
                        context: &self.context,
                    },
                    mutates,
                    batch_kind,
                    Some(attempt),
                    metadata_out,
                )
                .await?;
            self.decode_atomic_response(operations, response, mutates)
        })
    }

    fn access_mode(&self) -> AccessMode {
        self.access_mode
    }
}

fn check_parameters(params: &[Parameter]) -> Result<(), DactylError> {
    if params
        .iter()
        .any(|param| matches!(param, Parameter::Real(value) if !value.is_finite()))
    {
        return Err(DactylError::adapter_with_code(
            AdapterErrorKind::Value,
            "unsupported_value",
            "non-finite real parameters are unsupported",
        ));
    }
    Ok(())
}

fn ensure_writable(mode: AccessMode) -> Result<(), DactylError> {
    if mode == AccessMode::ReadOnly {
        Err(DactylError::adapter_with_code(
            AdapterErrorKind::ReadOnly,
            "read_only",
            "route is read-only",
        ))
    } else {
        Ok(())
    }
}

fn write_from_response(response: Response) -> WriteResult {
    WriteResult {
        affected_rows: response.affected_rows,
        generated_keys: response.generated_keys,
    }
}

fn rows_from_response_budgeted(
    response: Response,
    budget: ResourceBudget,
    tracker: &mut DecodedResultTracker,
) -> Result<Rows, DactylError> {
    let mut rows = Vec::with_capacity(response.rows.len());
    visit_response_rows(response, budget, tracker, &mut |row| {
        rows.push(row.clone());
        Ok(true)
    })?;
    Ok(Rows(rows))
}

fn visit_response_rows(
    response: Response,
    budget: ResourceBudget,
    tracker: &mut DecodedResultTracker,
    visitor: &mut dyn FnMut(&Row) -> Result<bool, DactylError>,
) -> Result<ScanOutcome, DactylError> {
    // Object rows cannot preserve two distinct values with the same alias.
    let names: std::collections::HashSet<_> = response.columns.iter().collect();
    if names.len() != response.columns.len() {
        return Err(protocol_error(
            "duplicate columns are not representable by the service row contract",
        ));
    }
    if response.rows.len() as u64 > budget.max_rows() {
        return Err(result_limit(
            "row_budget_exceeded",
            "decoded row count exceeds max_rows",
        ));
    }
    tracker.charge_columns(&response.columns)?;
    let mut visited = 0_u64;
    for value in response.rows {
        let object = value
            .as_object()
            .ok_or_else(|| protocol_error("Supabase row must be an object"))?;
        if object.len() != response.columns.len() {
            return Err(protocol_error("Supabase row does not match columns"));
        }
        tracker.charge_row_overhead(&response.columns)?;
        let mut values = Vec::with_capacity(response.columns.len());
        for name in &response.columns {
            let cell = object
                .get(name)
                .ok_or_else(|| protocol_error("Supabase row is missing a column"))?;
            let supported = match cell {
                serde_json::Value::Null
                | serde_json::Value::Bool(_)
                | serde_json::Value::String(_) => true,
                serde_json::Value::Number(number) => {
                    number.as_i64().is_some()
                        || (number.is_f64() && number.as_f64().is_some_and(f64::is_finite))
                }
                serde_json::Value::Array(bytes) => bytes
                    .iter()
                    .all(|byte| byte.as_u64().is_some_and(|n| n <= 255)),
                _ => false,
            };
            if !supported {
                return Err(DactylError::adapter_with_code(
                    AdapterErrorKind::Capability,
                    "unsupported_type",
                    "service returned a value outside the portable scalar/blob profile",
                ));
            }
            tracker.charge_json_value(cell)?;
            values.push(cell.clone());
        }
        let row = Row {
            columns: response.columns.clone(),
            values,
        };
        visited += 1;
        if !visitor(&row)? {
            return Ok(ScanOutcome {
                rows: visited,
                complete: false,
            });
        }
    }
    Ok(ScanOutcome {
        rows: visited,
        complete: true,
    })
}

fn protocol_error(message: &'static str) -> DactylError {
    DactylError::adapter(AdapterErrorKind::Protocol, message)
}

fn transport_error(timeout: bool) -> DactylError {
    DactylError::adapter(
        if timeout {
            AdapterErrorKind::Timeout
        } else {
            AdapterErrorKind::Transport
        },
        "Supabase service transport failed (details redacted)",
    )
}

fn remote_error(status: u16, body: &[u8], rpc: bool) -> DactylError {
    let envelope = serde_json::from_slice::<serde_json::Value>(body).ok();
    let code = envelope.as_ref().and_then(|value| {
        if rpc {
            value
                .get("code")
                .and_then(|v| v.as_str())
                .or_else(|| {
                    value
                        .get("error")
                        .and_then(|v| v.get("code"))
                        .and_then(|v| v.as_str())
                })
                .or_else(|| value.get("error_code").and_then(|v| v.as_str()))
                .or_else(|| value.get("error").and_then(|v| v.as_str()))
        } else {
            value
                .get("error")
                .and_then(|v| v.get("code"))
                .and_then(|v| v.as_str())
                .or_else(|| value.get("error_code").and_then(|v| v.as_str()))
                .or_else(|| value.get("code").and_then(|v| v.as_str()))
                .or_else(|| value.get("error").and_then(|v| v.as_str()))
        }
    });
    if rpc {
        let mapped = match code {
            Some("23505") => Some((AdapterErrorKind::Constraint, "unique_violation")),
            Some("23503") => Some((AdapterErrorKind::Constraint, "foreign_key_violation")),
            Some("23502") => Some((AdapterErrorKind::Constraint, "not_null_violation")),
            Some("23514") => Some((AdapterErrorKind::Constraint, "constraint_failed")),
            Some("40001" | "40P01") => Some((AdapterErrorKind::Conflict, "row_conflict")),
            Some("57014") => Some((AdapterErrorKind::Timeout, "timeout")),
            Some("25006") => Some((AdapterErrorKind::ReadOnly, "read_only")),
            Some("42501") if status == 401 => {
                Some((AdapterErrorKind::Authentication, "authentication_required"))
            }
            Some("42501") => Some((AdapterErrorKind::Authorization, "authorization_denied")),
            Some("PGRST106") => Some((AdapterErrorKind::Capability, "unsupported_schema")),
            Some("PGRST202") => Some((AdapterErrorKind::Capability, "unsupported_rpc")),
            Some("PGRST301" | "PGRST302" | "PGRST303") => {
                Some((AdapterErrorKind::Authentication, "invalid_token"))
            }
            _ => None,
        };
        if let Some((kind, code)) = mapped {
            return DactylError::adapter_with_code(
                kind,
                code,
                format!("Supabase RPC rejected request ({code})"),
            );
        }
    }
    // Only fixed public codes leave the boundary. Remote messages and unknown
    // codes may echo tokens, SQL parameters or database connection strings.
    let known = match code {
        Some("session_expired") => Some((AdapterErrorKind::Authentication, "session_expired")),
        Some("session_revoked") => Some((AdapterErrorKind::Authentication, "session_revoked")),
        Some("session_not_found") => Some((AdapterErrorKind::Authentication, "session_not_found")),
        Some("invalid_token") => Some((AdapterErrorKind::Authentication, "invalid_token")),
        Some("use_dpop_nonce") => Some((AdapterErrorKind::Authentication, "use_dpop_nonce")),
        Some("invalid_dpop_proof") => {
            Some((AdapterErrorKind::Authentication, "invalid_dpop_proof"))
        }
        Some("invalid_binding") => Some((AdapterErrorKind::Authentication, "invalid_binding")),
        Some("dpop_proof_required") => {
            Some((AdapterErrorKind::Authentication, "dpop_proof_required"))
        }
        Some("missing_dpop_proof") => {
            Some((AdapterErrorKind::Authentication, "missing_dpop_proof"))
        }
        Some("dpop_replay") => Some((AdapterErrorKind::Authentication, "dpop_replay")),
        Some("dpop_nonce_mismatch") => {
            Some((AdapterErrorKind::Authentication, "dpop_nonce_mismatch"))
        }
        Some("dpop_clock_skew") => Some((AdapterErrorKind::Authentication, "dpop_clock_skew")),
        Some("dpop_ath_mismatch") => Some((AdapterErrorKind::Authentication, "dpop_ath_mismatch")),
        Some("dpop_htu_mismatch") => Some((AdapterErrorKind::Authentication, "dpop_htu_mismatch")),
        Some("dpop_htm_mismatch") => Some((AdapterErrorKind::Authentication, "dpop_htm_mismatch")),
        Some("dpop_jkt_mismatch") => Some((AdapterErrorKind::Authentication, "dpop_jkt_mismatch")),
        Some("dpop_aud_mismatch") => Some((AdapterErrorKind::Authentication, "dpop_aud_mismatch")),
        Some("expired_credential") => {
            Some((AdapterErrorKind::Authentication, "expired_credential"))
        }
        Some("stolen_bearer") => Some((AdapterErrorKind::Authentication, "stolen_bearer")),
        Some("device_revoked") => Some((AdapterErrorKind::Authentication, "device_revoked")),
        Some("device_key_expired") => {
            Some((AdapterErrorKind::Authentication, "device_key_expired"))
        }
        Some("refresh_reuse_detected") => {
            Some((AdapterErrorKind::Authentication, "refresh_reuse_detected"))
        }
        Some("device_not_enrolled") => {
            Some((AdapterErrorKind::Authentication, "device_not_enrolled"))
        }
        Some("authorization_denied") => {
            Some((AdapterErrorKind::Authorization, "authorization_denied"))
        }
        Some("grant_revoked") => Some((AdapterErrorKind::Authorization, "grant_revoked")),
        Some("grant_expired") => Some((AdapterErrorKind::Authorization, "grant_expired")),
        Some("scope_denied") => Some((AdapterErrorKind::Authorization, "scope_denied")),
        Some("mfa_step_up_required") => {
            Some((AdapterErrorKind::Authorization, "mfa_step_up_required"))
        }
        Some("provider_disabled") => Some((AdapterErrorKind::Authorization, "provider_disabled")),
        Some("repo_access_denied") => Some((AdapterErrorKind::Authorization, "repo_access_denied")),
        Some("invalid_request") => Some((AdapterErrorKind::InvalidOperation, "invalid_request")),
        Some("unsupported_query") => Some((AdapterErrorKind::Capability, "unsupported_query")),
        Some("unsupported_type") => Some((AdapterErrorKind::Capability, "unsupported_type")),
        Some("unsupported_schema") => Some((AdapterErrorKind::Capability, "unsupported_schema")),
        Some("authentication_required") => {
            Some((AdapterErrorKind::Authentication, "authentication_required"))
        }
        Some("missing_context") => Some((AdapterErrorKind::Authentication, "missing_context")),
        Some("invalid_context") => Some((AdapterErrorKind::Protocol, "invalid_context")),
        Some("malformed_context") => Some((AdapterErrorKind::Protocol, "malformed_context")),
        Some("entitlement_required") => {
            Some((AdapterErrorKind::Authorization, "entitlement_required"))
        }
        Some("repository_not_authorized") => {
            Some((AdapterErrorKind::Authorization, "repository_not_authorized"))
        }
        Some("not_found") => Some((AdapterErrorKind::NotFound, "not_found")),
        Some("project_not_found") => Some((AdapterErrorKind::NotFound, "project_not_found")),
        Some("member_not_found") => Some((AdapterErrorKind::NotFound, "member_not_found")),
        Some("invitation_not_found") => Some((AdapterErrorKind::NotFound, "invitation_not_found")),
        Some("flow_not_found") => Some((AdapterErrorKind::NotFound, "flow_not_found")),
        Some("row_not_found") => Some((AdapterErrorKind::NotFound, "row_not_found")),
        Some("row_conflict") => Some((AdapterErrorKind::Conflict, "row_conflict")),
        Some("constraint_failed") => Some((AdapterErrorKind::Constraint, "constraint_failed")),
        Some("unique_violation") => Some((AdapterErrorKind::Constraint, "unique_violation")),
        Some("not_null_violation") => Some((AdapterErrorKind::Constraint, "not_null_violation")),
        Some("foreign_key_violation") => {
            Some((AdapterErrorKind::Constraint, "foreign_key_violation"))
        }
        Some("busy") => Some((AdapterErrorKind::Busy, "busy")),
        Some("locked") => Some((AdapterErrorKind::Locked, "locked")),
        Some("timeout") => Some((AdapterErrorKind::Timeout, "timeout")),
        Some("read_only") => Some((AdapterErrorKind::ReadOnly, "read_only")),
        Some("version_conflict") => Some((AdapterErrorKind::VersionConflict, "version_conflict")),
        Some("idempotency_conflict") => Some((
            AdapterErrorKind::IdempotencyConflict,
            "idempotency_conflict",
        )),
        Some("idempotency_in_progress") => Some((
            AdapterErrorKind::IdempotencyInProgress,
            "idempotency_in_progress",
        )),
        Some("transaction_aborted") => {
            Some((AdapterErrorKind::TransactionAborted, "transaction_aborted"))
        }
        Some("quota_exceeded") => Some((AdapterErrorKind::Quota, "quota_exceeded")),
        Some("overage_cap_exhausted") => Some((AdapterErrorKind::Quota, "overage_cap_exhausted")),
        Some("payload_too_large") => Some((AdapterErrorKind::Quota, "payload_too_large")),
        Some("rate_limited") => Some((AdapterErrorKind::RateLimited, "rate_limited")),
        Some("storage_failure") => Some((AdapterErrorKind::Storage, "storage_failure")),
        Some("storage_unavailable") => Some((AdapterErrorKind::Unavailable, "storage_unavailable")),
        _ => None,
    };
    if let Some((kind, code)) = known {
        return DactylError::adapter_with_code(
            kind,
            code,
            format!("Supabase service rejected request ({code})"),
        );
    }
    let kind = match status {
        300..=399 => AdapterErrorKind::Protocol,
        401 => AdapterErrorKind::Authentication,
        402 | 403 => AdapterErrorKind::Authorization,
        404 => AdapterErrorKind::NotFound,
        408 => AdapterErrorKind::Timeout,
        409 => AdapterErrorKind::Conflict,
        413 => AdapterErrorKind::Quota,
        429 => AdapterErrorKind::RateLimited,
        500 => AdapterErrorKind::Storage,
        502..=504 => AdapterErrorKind::Unavailable,
        _ => AdapterErrorKind::Query,
    };
    DactylError::adapter(
        kind,
        format!("Supabase service returned HTTP status {status}"),
    )
}
