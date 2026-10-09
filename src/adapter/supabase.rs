//! Authenticated HTTP transport for a Supabase-backed storage service.
//!
//! This adapter never connects to PostgreSQL or interprets tenant policy.

use std::io::Read;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::adapter::Adapter;
use crate::contract::{
    AccessMode, AtomicResult, GeneratedKey, OpenOptions, Operation, OperationKind, OperationResult,
    StorageContext, WriteResult,
};
use crate::error::{AdapterErrorKind, DactylError};
use crate::rows::{Parameter, Row, Rows};

const REQUEST_TIMEOUT: Duration = Duration::from_secs(15);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_BODY_BYTES: u64 = 4 * 1024 * 1024;

pub(crate) struct SupabaseAdapter {
    endpoint: String,
    bearer: reqwest::header::HeaderValue,
    access_mode: AccessMode,
    context: StorageContext,
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

impl SupabaseAdapter {
    pub(crate) fn new(
        endpoint: &str,
        bearer: Option<String>,
        options: OpenOptions,
        context: Option<StorageContext>,
    ) -> Result<Self, DactylError> {
        let url = validate_endpoint(endpoint)?;
        let bearer = bearer
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| {
                DactylError::adapter_with_code(
                    AdapterErrorKind::Authentication,
                    "authentication_required",
                    "Supabase service requires a user bearer token",
                )
            })?;
        let mut bearer = reqwest::header::HeaderValue::from_str(&format!("Bearer {bearer}"))
            .map_err(|_| DactylError::Config("invalid Supabase bearer token".into()))?;
        bearer.set_sensitive(true);
        let context = context.ok_or_else(|| {
            DactylError::adapter_with_code(
                AdapterErrorKind::Authentication,
                "missing_context",
                "Supabase service requires an authenticated storage context",
            )
        })?;
        context.validate()?;
        Ok(Self {
            endpoint: url.as_str().trim_end_matches('/').to_owned(),
            bearer,
            access_mode: options.access_mode,
            context,
        })
    }

    fn send<T: for<'de> Deserialize<'de> + Send + 'static>(
        &self,
        path: &str,
        request: &impl Serialize,
    ) -> Result<T, DactylError> {
        let body = serde_json::to_vec(request)
            .map_err(|_| DactylError::adapter(AdapterErrorKind::Value, "invalid request value"))?;
        if body.len() as u64 > MAX_BODY_BYTES {
            return Err(DactylError::adapter_with_code(
                AdapterErrorKind::Quota,
                "payload_too_large",
                "Supabase request exceeds the 4 MiB limit",
            ));
        }
        let endpoint = format!("{}{path}", self.endpoint);
        let bearer = self.bearer.clone();
        // The facade is synchronous, but callers may enter from an async
        // runtime. reqwest::blocking must be constructed, used AND dropped
        // outside that runtime. Keep the entire bounded request on this worker.
        std::thread::Builder::new()
            .name("dactyl-supabase-http".into())
            .spawn(move || send_request(endpoint, bearer, body))
            .map_err(|_| transport_error(false))?
            .join()
            .map_err(|_| transport_error(false))?
    }

    fn request(
        &self,
        sql: &str,
        params: &[Parameter],
        kind: OperationKind,
    ) -> Result<Response, DactylError> {
        check_parameters(params)?;
        self.send(
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
        )
    }
}

fn send_request<T: for<'de> Deserialize<'de>>(
    endpoint: String,
    bearer: reqwest::header::HeaderValue,
    body: Vec<u8>,
) -> Result<T, DactylError> {
    let client = reqwest::blocking::Client::builder()
        .connect_timeout(CONNECT_TIMEOUT)
        .timeout(REQUEST_TIMEOUT)
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .map_err(|_| transport_error(false))?;
    let response = client
        .post(endpoint)
        .header(reqwest::header::AUTHORIZATION, bearer)
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .body(body)
        .send()
        .map_err(|error| transport_error(error.is_timeout()))?;
    let status = response.status().as_u16();
    let mut bytes = Vec::new();
    response
        .take(MAX_BODY_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(|error| transport_error(body_read_timed_out(&error)))?;
    if bytes.len() as u64 > MAX_BODY_BYTES {
        return Err(protocol_error("Supabase response exceeds the 4 MiB limit"));
    }
    if !(200..300).contains(&status) {
        return Err(remote_error(status, &bytes));
    }
    serde_json::from_slice(&bytes).map_err(|_| protocol_error("invalid Supabase service response"))
}

fn body_read_timed_out(error: &std::io::Error) -> bool {
    // reqwest's Read implementation wraps its typed timeout in an Other I/O
    // error. Preserve that identity without inspecting potentially secret text.
    error.kind() == std::io::ErrorKind::TimedOut
        || error
            .get_ref()
            .and_then(|source| source.downcast_ref::<reqwest::Error>())
            .is_some_and(reqwest::Error::is_timeout)
}

impl Adapter for SupabaseAdapter {
    fn read(&self, sql: &str, params: &[Parameter]) -> Result<Rows, DactylError> {
        rows_from_response(self.request(sql, params, OperationKind::Read)?)
    }

    fn write(&self, sql: &str, params: &[Parameter]) -> Result<WriteResult, DactylError> {
        ensure_writable(self.access_mode)?;
        Ok(write_from_response(self.request(
            sql,
            params,
            OperationKind::Write,
        )?))
    }

    fn atomic(&self, operations: &[Operation]) -> Result<AtomicResult, DactylError> {
        if operations.is_empty() {
            return Ok(AtomicResult::default());
        }
        if operations
            .iter()
            .any(|operation| operation.kind() != OperationKind::Read)
        {
            ensure_writable(self.access_mode)?;
        }
        for operation in operations {
            check_parameters(operation.params())?;
        }
        let response: BatchResponse = self.send(
            "/batch",
            &BatchRequest {
                operations,
                access_mode: self.access_mode,
                context: &self.context,
            },
        )?;
        if response.results.len() != operations.len() {
            return Err(protocol_error(
                "Supabase batch result count does not match operation count",
            ));
        }
        let results = operations
            .iter()
            .zip(response.results)
            .map(|(operation, result)| match operation.kind() {
                OperationKind::Read => rows_from_response(result).map(OperationResult::Rows),
                OperationKind::Write | OperationKind::Schema => {
                    Ok(OperationResult::Write(write_from_response(result)))
                }
            })
            .collect::<Result<_, _>>()?;
        Ok(AtomicResult { results })
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

fn rows_from_response(response: Response) -> Result<Rows, DactylError> {
    // Object rows cannot preserve two distinct values with the same alias.
    let names: std::collections::HashSet<_> = response.columns.iter().collect();
    if names.len() != response.columns.len() {
        return Err(protocol_error(
            "duplicate columns are not representable by the service row contract",
        ));
    }
    let rows = response
        .rows
        .into_iter()
        .map(|value| {
            let object = value
                .as_object()
                .ok_or_else(|| protocol_error("Supabase row must be an object"))?;
            if object.len() != response.columns.len() {
                return Err(protocol_error("Supabase row does not match columns"));
            }
            let values = response
                .columns
                .iter()
                .map(|name| {
                    let value = object
                        .get(name)
                        .ok_or_else(|| protocol_error("Supabase row is missing a column"))?;
                    let supported = match value {
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
                    Ok(value.clone())
                })
                .collect::<Result<_, _>>()?;
            Ok(Row {
                columns: response.columns.clone(),
                values,
            })
        })
        .collect::<Result<_, _>>()?;
    Ok(Rows(rows))
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

fn remote_error(status: u16, body: &[u8]) -> DactylError {
    let envelope = serde_json::from_slice::<serde_json::Value>(body).ok();
    let code = envelope
        .as_ref()
        .and_then(|value| value.get("error")?.get("code")?.as_str());
    // Only fixed public codes leave the boundary. Remote messages and unknown
    // codes may echo tokens, SQL parameters or database connection strings.
    let known = match code {
        Some("invalid_token") => Some((AdapterErrorKind::Authentication, "invalid_token")),
        Some("authorization_denied") => {
            Some((AdapterErrorKind::Authorization, "authorization_denied"))
        }
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
