//! Minimal SQL-over-HTTP transport for Neon.

use serde::{Deserialize, Serialize};

use crate::adapter::Adapter;
use crate::contract::{
    AccessMode, AtomicResult, OpenOptions, Operation, OperationResult, StorageContext, WriteResult,
};
use crate::error::{AdapterErrorKind, DactylError};
use crate::rows::{Parameter, Row, Rows};

pub struct NeonAdapter {
    endpoint: String,
    bearer: Option<String>,
    client: reqwest::blocking::Client,
    access_mode: AccessMode,
    context: Option<StorageContext>,
}

#[derive(Debug, Serialize)]
struct Request<'a> {
    sql: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    params: Option<&'a [Parameter]>,
    access_mode: AccessMode,
    context: &'a StorageContext,
}

#[derive(Debug, Serialize)]
struct BatchRequest<'a> {
    access_mode: AccessMode,
    operations: &'a [Operation],
    context: &'a StorageContext,
}

#[derive(Debug, Deserialize)]
struct Response {
    #[serde(default)]
    columns: Vec<String>,
    #[serde(default)]
    rows: Vec<serde_json::Value>,
    #[serde(default)]
    affected_rows: Option<u64>,
    #[serde(default)]
    generated_keys: Vec<crate::contract::GeneratedKey>,
}

#[derive(Debug, Deserialize)]
struct ErrorEnvelope {
    error: ErrorBody,
}

#[derive(Debug, Deserialize)]
struct ErrorBody {
    code: Option<String>,
    message: Option<String>,
}

#[derive(Debug, Deserialize)]
struct BatchResponse {
    #[serde(default)]
    results: Vec<Response>,
}

impl NeonAdapter {
    #[allow(dead_code)]
    pub fn new(endpoint: &str, bearer: Option<String>) -> Self {
        Self::new_with_options(endpoint, bearer, OpenOptions::default(), None)
    }

    pub fn new_with_options(
        endpoint: &str,
        bearer: Option<String>,
        options: OpenOptions,
        context: Option<StorageContext>,
    ) -> Self {
        Self {
            endpoint: endpoint.trim_end_matches('/').to_owned(),
            bearer,
            client: reqwest::blocking::Client::new(),
            access_mode: options.access_mode,
            context,
        }
    }

    fn request(&self, sql: &str, params: &[Parameter]) -> Result<Response, DactylError> {
        let context = self.context()?;
        let mut request = self.client.post(format!("{}/query", self.endpoint));
        if let Some(token) = &self.bearer {
            request = request.bearer_auth(token);
        }
        let response = request
            .json(&Request {
                sql,
                params: Some(params),
                access_mode: self.access_mode,
                context,
            })
            .send()
            .map_err(|error| {
                DactylError::adapter(AdapterErrorKind::Transport, format!("neon send: {error}"))
            })?;
        decode_response(response, "neon")
    }

    fn request_batch(&self, operations: &[Operation]) -> Result<BatchResponse, DactylError> {
        let context = self.context()?;
        let mut request = self.client.post(format!("{}/batch", self.endpoint));
        if let Some(token) = &self.bearer {
            request = request.bearer_auth(token);
        }
        let response = request
            .json(&BatchRequest {
                access_mode: self.access_mode,
                operations,
                context,
            })
            .send()
            .map_err(|error| {
                DactylError::adapter(
                    AdapterErrorKind::Transport,
                    format!("neon batch send: {error}"),
                )
            })?;
        decode_response(response, "neon batch")
    }

    fn context(&self) -> Result<&StorageContext, DactylError> {
        let context = self.context.as_ref().ok_or_else(|| {
            DactylError::adapter_with_code(
                AdapterErrorKind::Authentication,
                "authentication_required",
                "neon requests require an authenticated storage context",
            )
        })?;
        context.validate()?;
        Ok(context)
    }
}

fn decode_response<T: for<'de> Deserialize<'de>>(
    response: reqwest::blocking::Response,
    operation: &str,
) -> Result<T, DactylError> {
    let status = response.status();
    let body = response.bytes().map_err(|error| {
        DactylError::adapter(
            AdapterErrorKind::Transport,
            format!("{operation} response: {error}"),
        )
    })?;
    if !status.is_success() {
        return Err(remote_error(status.as_u16(), &body, operation));
    }
    serde_json::from_slice(&body).map_err(|error| {
        DactylError::adapter(
            AdapterErrorKind::Protocol,
            format!("{operation} decode: {error}"),
        )
    })
}

fn remote_error(status: u16, body: &[u8], operation: &str) -> DactylError {
    let envelope = serde_json::from_slice::<ErrorEnvelope>(body).ok();
    let code = envelope.as_ref().and_then(|value| value.error.code.clone());
    let message = envelope
        .as_ref()
        .and_then(|value| value.error.message.clone())
        .unwrap_or_else(|| format!("{operation} returned HTTP status {status}"));
    let kind = match code.as_deref() {
        Some("invalid_request") => AdapterErrorKind::InvalidOperation,
        Some("unsupported_query") => AdapterErrorKind::Capability,
        Some("authentication_required") => AdapterErrorKind::Authentication,
        Some("missing_context") => AdapterErrorKind::Authentication,
        Some("invalid_context") | Some("malformed_context") => AdapterErrorKind::Protocol,
        Some("entitlement_required") | Some("repository_not_authorized") => {
            AdapterErrorKind::Authorization
        }
        Some("row_not_found") => AdapterErrorKind::NotFound,
        Some("row_conflict") => AdapterErrorKind::Conflict,
        Some("constraint_failed")
        | Some("unique_violation")
        | Some("not_null_violation")
        | Some("foreign_key_violation") => AdapterErrorKind::Constraint,
        Some("busy") => AdapterErrorKind::Busy,
        Some("locked") => AdapterErrorKind::Locked,
        Some("timeout") => AdapterErrorKind::Timeout,
        Some("version_conflict") => AdapterErrorKind::VersionConflict,
        Some("idempotency_conflict") => AdapterErrorKind::IdempotencyConflict,
        Some("idempotency_in_progress") => AdapterErrorKind::IdempotencyInProgress,
        Some("transaction_aborted") => AdapterErrorKind::TransactionAborted,
        Some("quota_exceeded") => AdapterErrorKind::Quota,
        Some("payload_too_large") => AdapterErrorKind::Quota,
        Some("rate_limited") => AdapterErrorKind::RateLimited,
        Some("storage_failure") => AdapterErrorKind::Storage,
        Some("storage_unavailable") => AdapterErrorKind::Unavailable,
        _ => match status {
            401 => AdapterErrorKind::Authentication,
            402 | 403 => AdapterErrorKind::Authorization,
            404 => AdapterErrorKind::NotFound,
            408 | 409 => AdapterErrorKind::Conflict,
            429 => AdapterErrorKind::RateLimited,
            500 => AdapterErrorKind::Storage,
            503 => AdapterErrorKind::Unavailable,
            _ => AdapterErrorKind::Query,
        },
    };
    match code {
        Some(code) => DactylError::adapter_with_code(kind, code, message),
        None => DactylError::adapter(kind, message),
    }
}

impl Adapter for NeonAdapter {
    fn read(&self, sql: &str, params: &[Parameter]) -> Result<Rows, DactylError> {
        rows_from_response(self.request(sql, params)?)
    }

    fn write(&self, sql: &str, params: &[Parameter]) -> Result<WriteResult, DactylError> {
        ensure_remote_writable(self.access_mode)?;
        let response = self.request(sql, params)?;
        Ok(WriteResult {
            affected_rows: response.affected_rows.unwrap_or(response.rows.len() as u64),
            generated_keys: response.generated_keys,
        })
    }

    fn atomic(&self, operations: &[Operation]) -> Result<AtomicResult, DactylError> {
        if operations.is_empty() {
            return Ok(AtomicResult::default());
        }
        if operations
            .iter()
            .any(|operation| operation.kind != crate::contract::OperationKind::Read)
        {
            ensure_remote_writable(self.access_mode)?;
        }
        let response = self.request_batch(operations)?;
        if response.results.len() != operations.len() {
            return Err(DactylError::adapter(
                AdapterErrorKind::Protocol,
                format!(
                    "neon batch returned {} results for {} operations",
                    response.results.len(),
                    operations.len()
                ),
            ));
        }
        let mut results = Vec::with_capacity(response.results.len());
        for result in response.results {
            if !result.columns.is_empty() || !result.rows.is_empty() {
                results.push(OperationResult::Rows(rows_from_response(result)?));
            } else {
                results.push(OperationResult::Write(WriteResult {
                    affected_rows: result.affected_rows.unwrap_or(0),
                    generated_keys: result.generated_keys,
                }));
            }
        }
        Ok(AtomicResult { results })
    }

    fn access_mode(&self) -> AccessMode {
        self.access_mode
    }

    fn inspect_schema(&self) -> Result<crate::schema::StoreSchema, DactylError> {
        Err(DactylError::adapter_with_code(
            AdapterErrorKind::Capability,
            "unsupported_schema_inspection",
            "schema inspection is a local-store operation",
        ))
    }
}

fn ensure_remote_writable(mode: AccessMode) -> Result<(), DactylError> {
    if mode == AccessMode::ReadOnly {
        Err(DactylError::adapter(
            AdapterErrorKind::ReadOnly,
            "route is read-only",
        ))
    } else {
        Ok(())
    }
}

fn rows_from_response(response: Response) -> Result<Rows, DactylError> {
    let mut rows = Vec::with_capacity(response.rows.len());
    for value in response.rows {
        let object = value.as_object().ok_or_else(|| {
            DactylError::adapter(AdapterErrorKind::Protocol, "neon row is not an object")
        })?;
        let columns = if response.columns.is_empty() {
            object.keys().cloned().collect::<Vec<_>>()
        } else {
            response.columns.clone()
        };
        let values = columns
            .iter()
            .map(|column| {
                object
                    .get(column)
                    .cloned()
                    .unwrap_or(serde_json::Value::Null)
            })
            .collect();
        rows.push(Row { columns, values });
    }
    Ok(Rows(rows))
}
