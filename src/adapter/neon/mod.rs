//! Minimal SQL-over-HTTP transport for Neon with bounded wire and result budgets.

use std::sync::Arc;
use std::time::Instant;

use serde::{Deserialize, Serialize};

use crate::adapter::{Adapter, AdapterFuture};
use crate::budget::{
    ensure_json_depth, mark_remote_uncertain, preflight_batch, preflight_statement,
    read_bounded_body, result_limit, serialize_bounded_json, BoundedReadError,
    DecodedResultTracker, ResourceBudget,
};
use crate::contract::{
    AccessMode, AtomicResult, OpenOptions, Operation, OperationKind, OperationResult,
    StorageContext, WriteResult,
};
use crate::error::{AdapterErrorKind, DactylError};
use crate::http_pool::{
    acquire_client, in_tokio_runtime, read_bounded_body_async, AsyncBodyError, SharedHttpClient,
};
use crate::inspection::ScanOutcome;
use crate::rows::{Parameter, Row, Rows};

pub struct NeonAdapter {
    endpoint: String,
    bearer: Option<String>,
    access_mode: AccessMode,
    context: Option<StorageContext>,
    budget: ResourceBudget,
    http_client: Arc<SharedHttpClient>,
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
    pub fn new(endpoint: &str, bearer: Option<String>) -> Result<Self, DactylError> {
        Self::new_with_options(endpoint, bearer, OpenOptions::default(), None)
    }

    #[allow(dead_code)]
    pub fn new_with_options(
        endpoint: &str,
        bearer: Option<String>,
        options: OpenOptions,
        context: Option<StorageContext>,
    ) -> Result<Self, DactylError> {
        Self::new_with_options_and_budget(
            endpoint,
            bearer,
            options,
            context,
            ResourceBudget::default(),
        )
    }

    pub fn new_with_options_and_budget(
        endpoint: &str,
        bearer: Option<String>,
        options: OpenOptions,
        context: Option<StorageContext>,
        budget: ResourceBudget,
    ) -> Result<Self, DactylError> {
        budget.validate()?;
        let http_client = acquire_client(budget)?;
        Ok(Self {
            endpoint: endpoint.trim_end_matches('/').to_owned(),
            bearer,
            access_mode: options.access_mode,
            context,
            budget,
            http_client,
        })
    }

    fn prepare_send(
        &self,
        path: &str,
        request: &impl Serialize,
    ) -> Result<(String, Option<reqwest::header::HeaderValue>, Vec<u8>), DactylError> {
        let body = serialize_bounded_json(request, self.budget.max_wire_bytes())?;
        let bearer = self
            .bearer
            .as_deref()
            .map(|token| {
                let mut value = reqwest::header::HeaderValue::from_str(&format!("Bearer {token}"))
                    .map_err(|_| DactylError::Config("invalid Neon bearer token".into()))?;
                value.set_sensitive(true);
                Ok::<_, DactylError>(value)
            })
            .transpose()?;
        let endpoint = format!("{}{path}", self.endpoint);
        Ok((endpoint, bearer, body))
    }

    fn send<T: for<'de> Deserialize<'de> + Send + 'static>(
        &self,
        path: &str,
        operation: &'static str,
        request: &impl Serialize,
        mutates: bool,
    ) -> Result<T, DactylError> {
        let (endpoint, bearer, body) = self.prepare_send(path, request)?;
        let budget = self.budget;
        let http_client = self.http_client.clone();
        if in_tokio_runtime() {
            std::thread::Builder::new()
                .name("dactyl-neon-http".into())
                .spawn(move || {
                    send_neon_request(
                        &http_client,
                        endpoint,
                        bearer,
                        body,
                        budget,
                        operation,
                        mutates,
                    )
                })
                .map_err(|_| neon_transport_error(false))?
                .join()
                .map_err(|_| maybe_uncertain(neon_transport_error(false), mutates))?
        } else {
            send_neon_request(
                &http_client,
                endpoint,
                bearer,
                body,
                budget,
                operation,
                mutates,
            )
        }
    }

    async fn send_async<T: for<'de> Deserialize<'de> + Send + 'static>(
        &self,
        path: &str,
        operation: &'static str,
        request: &impl Serialize,
        mutates: bool,
    ) -> Result<T, DactylError> {
        let (endpoint, bearer, body) = self.prepare_send(path, request)?;
        send_neon_request_async(
            &self.http_client,
            endpoint,
            bearer,
            body,
            self.budget,
            operation,
            mutates,
        )
        .await
    }

    fn prepare_query<'a>(
        &'a self,
        sql: &'a str,
        params: &'a [Parameter],
        mutates: bool,
    ) -> Result<Request<'a>, DactylError> {
        let context = self.context()?;
        check_parameters(params)?;
        preflight_statement(&self.budget, sql, params)?;
        Ok(Request {
            sql,
            params: Some(params),
            access_mode: if mutates {
                self.access_mode
            } else {
                AccessMode::ReadOnly
            },
            context,
        })
    }

    fn request(
        &self,
        sql: &str,
        params: &[Parameter],
        mutates: bool,
    ) -> Result<Response, DactylError> {
        let req = self.prepare_query(sql, params, mutates)?;
        self.send("/query", "neon", &req, mutates)
    }

    async fn request_async(
        &self,
        sql: &str,
        params: &[Parameter],
        mutates: bool,
    ) -> Result<Response, DactylError> {
        let req = self.prepare_query(sql, params, mutates)?;
        self.send_async("/query", "neon", &req, mutates).await
    }

    fn prepare_batch<'a>(
        &'a self,
        operations: &'a [Operation],
        mutates: bool,
    ) -> Result<BatchRequest<'a>, DactylError> {
        let context = self.context()?;
        for operation in operations {
            check_parameters(operation.params())?;
        }
        preflight_batch(&self.budget, operations)?;
        Ok(BatchRequest {
            access_mode: if mutates {
                self.access_mode
            } else {
                AccessMode::ReadOnly
            },
            operations,
            context,
        })
    }

    fn request_batch(
        &self,
        operations: &[Operation],
        mutates: bool,
    ) -> Result<BatchResponse, DactylError> {
        let req = self.prepare_batch(operations, mutates)?;
        self.send("/batch", "neon batch", &req, mutates)
    }

    async fn request_batch_async(
        &self,
        operations: &[Operation],
        mutates: bool,
    ) -> Result<BatchResponse, DactylError> {
        let req = self.prepare_batch(operations, mutates)?;
        self.send_async("/batch", "neon batch", &req, mutates).await
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

fn maybe_uncertain(error: DactylError, mutates: bool) -> DactylError {
    if mutates {
        mark_remote_uncertain(error)
    } else {
        error
    }
}

fn decode_neon_response<T: for<'de> Deserialize<'de>>(
    status: u16,
    bytes: &[u8],
    operation: &'static str,
    mutates: bool,
) -> Result<T, DactylError> {
    if !(200..300).contains(&status) {
        let error = remote_error(status, bytes, operation);
        return if mutates && matches!(status, 408 | 502 | 504) {
            Err(mark_remote_uncertain(error))
        } else {
            Err(error)
        };
    }
    let value: serde_json::Value = serde_json::from_slice(bytes).map_err(|_| {
        maybe_uncertain(
            DactylError::adapter_with_code(
                AdapterErrorKind::Protocol,
                "malformed_response",
                format!("{operation} returned invalid JSON"),
            ),
            mutates,
        )
    })?;
    ensure_json_depth(&value).map_err(|error| maybe_uncertain(error, mutates))?;
    serde_json::from_value(value).map_err(|_| {
        maybe_uncertain(
            DactylError::adapter_with_code(
                AdapterErrorKind::Protocol,
                "malformed_response",
                format!("{operation} response envelope did not match contract"),
            ),
            mutates,
        )
    })
}

fn pre_send_timeout_error() -> DactylError {
    DactylError::adapter(
        AdapterErrorKind::Timeout,
        "local preflight: request deadline expired before dispatch",
    )
}

fn send_neon_request<T: for<'de> Deserialize<'de>>(
    http_client: &SharedHttpClient,
    endpoint: String,
    bearer: Option<reqwest::header::HeaderValue>,
    body: Vec<u8>,
    budget: ResourceBudget,
    operation: &'static str,
    mutates: bool,
) -> Result<T, DactylError> {
    let client = http_client.blocking_client()?;
    let deadline = Instant::now() + budget.request_timeout();
    let _permit = http_client.acquire_permit(deadline)?;
    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        return Err(pre_send_timeout_error());
    }
    let mut request = client
        .post(endpoint)
        .timeout(remaining)
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .body(body);
    if let Some(bearer) = bearer {
        request = request.header(reqwest::header::AUTHORIZATION, bearer);
    }
    let response = request
        .send()
        .map_err(|error| maybe_uncertain(neon_transport_error(error.is_timeout()), mutates))?;
    let status = response.status().as_u16();
    let bytes = match read_bounded_body(response, budget.max_wire_bytes()) {
        Ok(bytes) => bytes,
        Err(BoundedReadError::TooLarge) => {
            return Err(maybe_uncertain(
                DactylError::adapter_with_code(
                    AdapterErrorKind::Protocol,
                    "response_too_large",
                    "Neon response exceeds max_wire_bytes",
                ),
                mutates,
            ))
        }
        Err(BoundedReadError::Io(error)) => {
            return Err(maybe_uncertain(
                neon_transport_error(body_read_timed_out(&error)),
                mutates,
            ))
        }
    };
    decode_neon_response(status, &bytes, operation, mutates)
}

async fn send_neon_request_async<T: for<'de> Deserialize<'de>>(
    http_client: &SharedHttpClient,
    endpoint: String,
    bearer: Option<reqwest::header::HeaderValue>,
    body: Vec<u8>,
    budget: ResourceBudget,
    operation: &'static str,
    mutates: bool,
) -> Result<T, DactylError> {
    let client = http_client.async_client()?;
    let deadline = Instant::now() + budget.request_timeout();
    let _permit = http_client.acquire_permit_async(deadline).await?;
    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        return Err(pre_send_timeout_error());
    }
    let mut request = client
        .post(endpoint)
        .timeout(remaining)
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .body(body);
    if let Some(bearer) = bearer {
        request = request.header(reqwest::header::AUTHORIZATION, bearer);
    }
    let response = tokio::time::timeout(remaining, request.send())
        .await
        .map_err(|_| maybe_uncertain(neon_transport_error(true), mutates))?
        .map_err(|error| maybe_uncertain(neon_transport_error(error.is_timeout()), mutates))?;
    let status = response.status().as_u16();
    let body_remaining = deadline.saturating_duration_since(Instant::now());
    let bytes = match tokio::time::timeout(
        body_remaining,
        read_bounded_body_async(response, budget.max_wire_bytes()),
    )
    .await
    {
        Ok(Ok(bytes)) => bytes,
        Ok(Err(AsyncBodyError::TooLarge)) => {
            return Err(maybe_uncertain(
                DactylError::adapter_with_code(
                    AdapterErrorKind::Protocol,
                    "response_too_large",
                    "Neon response exceeds max_wire_bytes",
                ),
                mutates,
            ))
        }
        Ok(Err(AsyncBodyError::Transport(error))) => {
            return Err(maybe_uncertain(
                neon_transport_error(error.is_timeout()),
                mutates,
            ))
        }
        Err(_) => return Err(maybe_uncertain(neon_transport_error(true), mutates)),
    };
    decode_neon_response(status, &bytes, operation, mutates)
}

fn body_read_timed_out(error: &std::io::Error) -> bool {
    error.kind() == std::io::ErrorKind::TimedOut
        || error
            .get_ref()
            .and_then(|source| source.downcast_ref::<reqwest::Error>())
            .is_some_and(reqwest::Error::is_timeout)
}

fn neon_transport_error(timeout: bool) -> DactylError {
    DactylError::adapter(
        if timeout {
            AdapterErrorKind::Timeout
        } else {
            AdapterErrorKind::Transport
        },
        "Neon service transport failed (details redacted)",
    )
}

fn remote_error(status: u16, body: &[u8], operation: &str) -> DactylError {
    let envelope = serde_json::from_slice::<ErrorEnvelope>(body).ok();
    let code = envelope.as_ref().and_then(|value| value.error.code.clone());
    let _ = envelope
        .as_ref()
        .and_then(|value| value.error.message.as_ref());
    let kind = match code.as_deref() {
        Some("invalid_request") => AdapterErrorKind::InvalidOperation,
        Some("unsupported_query") | Some("unsupported_type") | Some("unsupported_schema") => {
            AdapterErrorKind::Capability
        }
        Some("authentication_required") | Some("invalid_token") => AdapterErrorKind::Authentication,
        Some("missing_context") => AdapterErrorKind::Authentication,
        Some("invalid_context") | Some("malformed_context") => AdapterErrorKind::Protocol,
        Some("authorization_denied")
        | Some("entitlement_required")
        | Some("repository_not_authorized") => AdapterErrorKind::Authorization,
        Some("row_not_found") => AdapterErrorKind::NotFound,
        Some("row_conflict") => AdapterErrorKind::Conflict,
        Some("constraint_failed")
        | Some("unique_violation")
        | Some("not_null_violation")
        | Some("foreign_key_violation") => AdapterErrorKind::Constraint,
        Some("busy") => AdapterErrorKind::Busy,
        Some("locked") => AdapterErrorKind::Locked,
        Some("timeout") => AdapterErrorKind::Timeout,
        Some("read_only") => AdapterErrorKind::ReadOnly,
        Some("version_conflict") => AdapterErrorKind::VersionConflict,
        Some("idempotency_conflict") => AdapterErrorKind::IdempotencyConflict,
        Some("idempotency_in_progress") => AdapterErrorKind::IdempotencyInProgress,
        Some("transaction_aborted") => AdapterErrorKind::TransactionAborted,
        Some("quota_exceeded") | Some("payload_too_large") => AdapterErrorKind::Quota,
        Some("rate_limited") => AdapterErrorKind::RateLimited,
        Some("storage_failure") => AdapterErrorKind::Storage,
        Some("storage_unavailable") => AdapterErrorKind::Unavailable,
        _ => match status {
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
        },
    };
    match code {
        Some(code) => DactylError::adapter_with_code(
            kind,
            code.clone(),
            format!("Neon service rejected request ({code})"),
        ),
        None => DactylError::adapter(kind, format!("{operation} returned HTTP status {status}")),
    }
}

fn finalize_write_response(
    response: Response,
    budget: ResourceBudget,
) -> Result<WriteResult, DactylError> {
    if !response.rows.is_empty() {
        let mut tracker = DecodedResultTracker::new(budget);
        let auxiliary = Response {
            columns: response.columns,
            rows: response.rows.clone(),
            affected_rows: response.affected_rows,
            generated_keys: Vec::new(),
        };
        rows_from_response_budgeted(auxiliary, budget, &mut tracker)
            .map_err(mark_remote_uncertain)?;
    }
    Ok(WriteResult {
        affected_rows: response.affected_rows.unwrap_or(response.rows.len() as u64),
        generated_keys: response.generated_keys,
    })
}

fn finalize_batch_response(
    operations: &[Operation],
    response: BatchResponse,
    budget: ResourceBudget,
    mutates: bool,
) -> Result<AtomicResult, DactylError> {
    if response.results.len() != operations.len() {
        return Err(maybe_uncertain(
            DactylError::adapter_with_code(
                AdapterErrorKind::Protocol,
                "malformed_response",
                format!(
                    "neon batch returned {} results for {} operations",
                    response.results.len(),
                    operations.len()
                ),
            ),
            mutates,
        ));
    }
    let mut tracker = DecodedResultTracker::new(budget);
    let mut results = Vec::with_capacity(response.results.len());
    for (operation, result) in operations.iter().zip(response.results) {
        match operation.kind() {
            OperationKind::Read => {
                let rows = rows_from_response_budgeted(result, budget, &mut tracker)
                    .map_err(|error| maybe_uncertain(error, mutates))?;
                results.push(OperationResult::Rows(rows));
            }
            OperationKind::Write | OperationKind::Schema => {
                results.push(OperationResult::Write(WriteResult {
                    affected_rows: result.affected_rows.unwrap_or(0),
                    generated_keys: result.generated_keys,
                }));
            }
        }
    }
    Ok(AtomicResult { results })
}

impl Adapter for NeonAdapter {
    fn read(&self, sql: &str, params: &[Parameter]) -> Result<Rows, DactylError> {
        let response = self.request(sql, params, false)?;
        let mut tracker = DecodedResultTracker::new(self.budget);
        rows_from_response_budgeted(response, self.budget, &mut tracker)
    }

    fn read_async<'a>(&'a self, sql: &'a str, params: &'a [Parameter]) -> AdapterFuture<'a, Rows> {
        Box::pin(async move {
            let response = self.request_async(sql, params, false).await?;
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
        let response = self.request(sql, params, false)?;
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
            let response = self.request_async(sql, params, false).await?;
            let mut tracker = DecodedResultTracker::new(self.budget);
            visit_response_rows(response, self.budget, &mut tracker, visitor)
        })
    }

    fn write(&self, sql: &str, params: &[Parameter]) -> Result<WriteResult, DactylError> {
        ensure_remote_writable(self.access_mode)?;
        let response = self.request(sql, params, true)?;
        finalize_write_response(response, self.budget)
    }

    fn write_async<'a>(
        &'a self,
        sql: &'a str,
        params: &'a [Parameter],
    ) -> AdapterFuture<'a, WriteResult> {
        Box::pin(async move {
            ensure_remote_writable(self.access_mode)?;
            let response = self.request_async(sql, params, true).await?;
            finalize_write_response(response, self.budget)
        })
    }

    fn atomic(&self, operations: &[Operation]) -> Result<AtomicResult, DactylError> {
        if operations.is_empty() {
            return Ok(AtomicResult::default());
        }
        let mutates = operations
            .iter()
            .any(|operation| operation.kind != crate::contract::OperationKind::Read);
        if mutates {
            ensure_remote_writable(self.access_mode)?;
        }
        let response = self.request_batch(operations, mutates)?;
        finalize_batch_response(operations, response, self.budget, mutates)
    }

    fn atomic_async<'a>(&'a self, operations: &'a [Operation]) -> AdapterFuture<'a, AtomicResult> {
        Box::pin(async move {
            if operations.is_empty() {
                return Ok(AtomicResult::default());
            }
            let mutates = operations
                .iter()
                .any(|operation| operation.kind != crate::contract::OperationKind::Read);
            if mutates {
                ensure_remote_writable(self.access_mode)?;
            }
            let response = self.request_batch_async(operations, mutates).await?;
            finalize_batch_response(operations, response, self.budget, mutates)
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

fn ensure_remote_writable(mode: AccessMode) -> Result<(), DactylError> {
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
    if response.rows.len() as u64 > budget.max_rows() {
        return Err(result_limit(
            "row_budget_exceeded",
            "decoded row count exceeds max_rows",
        ));
    }
    let mut charged_header = false;
    if !response.columns.is_empty() {
        let names: std::collections::HashSet<_> = response.columns.iter().collect();
        if names.len() != response.columns.len() {
            return Err(DactylError::adapter_with_code(
                AdapterErrorKind::Protocol,
                "malformed_response",
                "duplicate columns are not representable by the service row contract",
            ));
        }
        tracker.charge_columns(&response.columns)?;
        charged_header = true;
    }
    let mut visited = 0_u64;
    for value in response.rows {
        let object = value.as_object().ok_or_else(|| {
            DactylError::adapter_with_code(
                AdapterErrorKind::Protocol,
                "malformed_response",
                "neon row is not an object",
            )
        })?;
        let columns = if response.columns.is_empty() {
            let inferred = object.keys().cloned().collect::<Vec<_>>();
            if !charged_header {
                tracker.charge_columns(&inferred)?;
                charged_header = true;
            }
            inferred
        } else {
            response.columns.clone()
        };
        tracker.charge_row_overhead(&columns)?;
        let mut values = Vec::with_capacity(columns.len());
        for column in &columns {
            let cell = object
                .get(column)
                .cloned()
                .unwrap_or(serde_json::Value::Null);
            tracker.charge_json_value(&cell)?;
            values.push(cell);
        }
        let row = Row { columns, values };
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
