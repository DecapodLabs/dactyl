//! Authenticated HTTP transport for a Supabase-backed storage service.
//!
//! This adapter never connects to PostgreSQL or interprets tenant policy.

use serde::{Deserialize, Serialize};

use crate::adapter::Adapter;
use crate::budget::{
    ensure_json_depth, mark_remote_uncertain, preflight_batch, preflight_limit,
    preflight_statement, read_bounded_body, result_limit, serialize_bounded_json, BoundedReadError,
    DecodedResultTracker, ResourceBudget,
};
use crate::contract::{
    AccessMode, AtomicResult, GeneratedKey, OpenOptions, Operation, OperationKind, OperationResult,
    StorageContext, WriteResult,
};
use crate::error::{AdapterErrorKind, DactylError};
use crate::inspection::ScanOutcome;
use crate::rows::{Parameter, Row, Rows};
use crate::{SupabaseRpcConfig, SUPABASE_RPC_CONTRACT_VERSION};

pub(crate) struct SupabaseAdapter {
    endpoint: String,
    bearer: reqwest::header::HeaderValue,
    access_mode: AccessMode,
    context: StorageContext,
    rpc_headers: Option<reqwest::header::HeaderMap>,
    budget: ResourceBudget,
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

/// The application supplies a trusted function identifier, never a URL or SQL.
/// Policy and the JSON argument/result remain opaque to the driver.
pub(crate) fn control_request(
    config: &SupabaseRpcConfig,
    function: &str,
    request: &serde_json::Value,
    bearer: Option<&str>,
    kind: OperationKind,
    budget: ResourceBudget,
) -> Result<serde_json::Value, DactylError> {
    let origin = validate_rpc(config)?;
    budget.validate()?;
    let name = function.as_bytes();
    if name.is_empty()
        || name.len() > 63
        || !(name[0].is_ascii_alphabetic() || name[0] == b'_')
        || !name.iter().all(|b| b.is_ascii_alphanumeric() || *b == b'_')
    {
        return Err(DactylError::Config(
            "control RPC requires a function identifier".into(),
        ));
    }
    let token = bearer.ok_or_else(|| {
        DactylError::adapter_with_code(
            AdapterErrorKind::Authentication,
            "authentication_required",
            "local preflight: control RPC requires a user bearer token",
        )
    })?;
    crate::connector::validate_bearer_token(token)?;
    let mut bearer = reqwest::header::HeaderValue::from_str(&format!("Bearer {token}"))
        .map_err(|_| DactylError::Config("invalid managed session bearer".into()))?;
    bearer.set_sensitive(true);
    // Bound recursion before invoking serde's recursive serializer. Account for
    // the native single-argument envelope without cloning application data.
    ensure_json_depth(request).map_err(|_| {
        preflight_limit(
            "payload_too_deep",
            "control request exceeds JSON nesting limit",
        )
    })?;
    #[derive(Serialize)]
    struct ControlRequest<'a> {
        request: &'a serde_json::Value,
    }
    let body = serialize_bounded_json(&ControlRequest { request }, budget.max_wire_bytes())?;
    let headers = Some(rpc_headers(config)?);
    let endpoint = format!(
        "{}/rest/v1/rpc/{function}",
        origin.as_str().trim_end_matches('/')
    );
    let mutates = kind == OperationKind::Write;
    std::thread::Builder::new()
        .name("dactyl-control-rpc".into())
        .spawn(move || send_request(endpoint, bearer, body, headers, budget, mutates, false))
        .map_err(|_| transport_error(false))?
        .join()
        .map_err(|_| maybe_uncertain(transport_error(false), mutates))?
        .map_err(|error| {
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
    pub(crate) fn new(
        endpoint: &str,
        bearer: Option<String>,
        options: OpenOptions,
        context: Option<StorageContext>,
        rpc: Option<&SupabaseRpcConfig>,
        budget: ResourceBudget,
    ) -> Result<Self, DactylError> {
        budget.validate()?;
        let url = match rpc {
            Some(config) => validate_rpc(config)?,
            None => validate_endpoint(endpoint)?,
        };
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
        let rpc_headers = rpc.map(rpc_headers).transpose()?;
        Ok(Self {
            rpc_headers,
            endpoint: url.as_str().trim_end_matches('/').to_owned(),
            bearer,
            access_mode: options.access_mode,
            context,
            budget,
        })
    }

    fn send<T: for<'de> Deserialize<'de> + Send + 'static>(
        &self,
        path: &str,
        request: &impl Serialize,
        mutates: bool,
    ) -> Result<T, DactylError> {
        let max_wire_bytes = self.budget.max_wire_bytes();
        let body = if self.rpc_headers.is_some() {
            serialize_bounded_json(
                &RpcRequest {
                    request: VersionedRequest {
                        version: SUPABASE_RPC_CONTRACT_VERSION,
                        body: request,
                    },
                },
                max_wire_bytes,
            )?
        } else {
            serialize_bounded_json(request, max_wire_bytes)?
        };
        let path = if self.rpc_headers.is_some() {
            match path {
                "/query" => "/rest/v1/rpc/dactyl_query_v1",
                "/batch" if self.access_mode == AccessMode::ReadOnly => {
                    "/rest/v1/rpc/dactyl_read_batch_v1"
                }
                "/batch" => "/rest/v1/rpc/dactyl_batch_v1",
                _ => return Err(protocol_error("unsupported RPC operation")),
            }
        } else {
            path
        };
        let endpoint = format!("{}{path}", self.endpoint);
        let rpc_headers = self.rpc_headers.clone();
        let bearer = self.bearer.clone();
        let budget = self.budget;
        // The facade is synchronous, but callers may enter from an async
        // runtime. reqwest::blocking must be constructed, used AND dropped
        // outside that runtime. Keep the entire bounded request on this worker.
        std::thread::Builder::new()
            .name("dactyl-supabase-http".into())
            .spawn(move || send_request(endpoint, bearer, body, rpc_headers, budget, mutates, true))
            .map_err(|_| transport_error(false))?
            .join()
            .map_err(|_| maybe_uncertain(transport_error(false), mutates))?
    }

    fn request(
        &self,
        sql: &str,
        params: &[Parameter],
        kind: OperationKind,
        mutates: bool,
    ) -> Result<Response, DactylError> {
        check_parameters(params)?;
        preflight_statement(&self.budget, sql, params)?;
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
            mutates,
        )
    }
}

fn maybe_uncertain(error: DactylError, mutates: bool) -> DactylError {
    if mutates {
        mark_remote_uncertain(error)
    } else {
        error
    }
}

fn send_request<T: for<'de> Deserialize<'de>>(
    endpoint: String,
    bearer: reqwest::header::HeaderValue,
    body: Vec<u8>,
    rpc_headers: Option<reqwest::header::HeaderMap>,
    budget: ResourceBudget,
    mutates: bool,
    validate_rows: bool,
) -> Result<T, DactylError> {
    let client = reqwest::blocking::Client::builder()
        .connect_timeout(budget.connect_timeout())
        .timeout(budget.request_timeout())
        .redirect(reqwest::redirect::Policy::none())
        .retry(reqwest::retry::never())
        .build()
        .map_err(|_| transport_error(false))?;
    let rpc = rpc_headers.is_some();
    let response = client
        .post(endpoint)
        .headers(rpc_headers.unwrap_or_default())
        .header(reqwest::header::AUTHORIZATION, bearer)
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .body(body)
        .send()
        .map_err(|error| maybe_uncertain(transport_error(error.is_timeout()), mutates))?;
    let status = response.status().as_u16();
    let bytes = match read_bounded_body(response, budget.max_wire_bytes()) {
        Ok(bytes) => bytes,
        Err(BoundedReadError::TooLarge) => {
            return Err(maybe_uncertain(
                DactylError::adapter_with_code(
                    AdapterErrorKind::Protocol,
                    "response_too_large",
                    "Supabase response exceeds max_wire_bytes",
                ),
                mutates,
            ))
        }
        Err(BoundedReadError::Io(error)) => {
            return Err(maybe_uncertain(
                transport_error(body_read_timed_out(&error)),
                mutates,
            ))
        }
    };
    if !(200..300).contains(&status) {
        let error = remote_error(status, &bytes, rpc);
        return if mutates
            && (matches!(status, 408 | 502 | 504)
                || (!validate_rows && (500..600).contains(&status)))
        {
            Err(mark_remote_uncertain(error))
        } else {
            Err(error)
        };
    }
    let value: serde_json::Value = serde_json::from_slice(&bytes).map_err(|_| {
        maybe_uncertain(
            protocol_error(if rpc {
                "invalid RPC response"
            } else {
                "invalid Supabase service response"
            }),
            mutates,
        )
    })?;
    ensure_json_depth(&value).map_err(|error| maybe_uncertain(error, mutates))?;
    if rpc && validate_rows {
        if let Some(results) = value.get("results") {
            let mut tracker = DecodedResultTracker::new(budget);
            for result in results.as_array().ok_or_else(|| {
                maybe_uncertain(protocol_error("invalid RPC batch response"), mutates)
            })? {
                validate_rpc_result(result, budget, &mut tracker)
                    .map_err(|error| maybe_uncertain(error, mutates))?;
            }
        } else {
            let mut tracker = DecodedResultTracker::new(budget);
            validate_rpc_result(&value, budget, &mut tracker)
                .map_err(|error| maybe_uncertain(error, mutates))?;
        }
        return serde_json::from_value(value)
            .map_err(|_| maybe_uncertain(protocol_error("invalid RPC response"), mutates));
    }
    serde_json::from_value(value)
        .map_err(|_| maybe_uncertain(protocol_error("invalid Supabase service response"), mutates))
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

impl Adapter for SupabaseAdapter {
    fn read(&self, sql: &str, params: &[Parameter]) -> Result<Rows, DactylError> {
        let response = self.request(sql, params, OperationKind::Read, false)?;
        let mut tracker = DecodedResultTracker::new(self.budget);
        rows_from_response_budgeted(response, self.budget, &mut tracker)
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

    fn write(&self, sql: &str, params: &[Parameter]) -> Result<WriteResult, DactylError> {
        ensure_writable(self.access_mode)?;
        let response = self.request(sql, params, OperationKind::Write, true)?;
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

    fn atomic(&self, operations: &[Operation]) -> Result<AtomicResult, DactylError> {
        if operations.is_empty() {
            return Ok(AtomicResult::default());
        }
        let mutates = operations
            .iter()
            .any(|operation| operation.kind() != OperationKind::Read);
        if mutates {
            ensure_writable(self.access_mode)?;
        }
        for operation in operations {
            check_parameters(operation.params())?;
        }
        preflight_batch(&self.budget, operations)?;
        let response: BatchResponse = self.send(
            "/batch",
            &BatchRequest {
                operations,
                access_mode: self.access_mode,
                context: &self.context,
            },
            mutates,
        )?;
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
            value.get("code")?.as_str()
        } else {
            value.get("error")?.get("code")?.as_str()
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
