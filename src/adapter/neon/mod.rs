//! Minimal SQL-over-HTTP transport for Neon.

use serde::{Deserialize, Serialize};

use crate::adapter::Adapter;
use crate::contract::{
    AccessMode, AtomicResult, OpenOptions, Operation, OperationResult, WriteResult,
};
use crate::error::{AdapterErrorKind, DactylError};
use crate::rows::{Parameter, Row, Rows};

pub struct NeonAdapter {
    endpoint: String,
    bearer: Option<String>,
    client: reqwest::blocking::Client,
    access_mode: AccessMode,
}

#[derive(Debug, Serialize)]
struct Request<'a> {
    sql: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    params: Option<&'a [Parameter]>,
    access_mode: AccessMode,
}

#[derive(Debug, Serialize)]
struct BatchRequest<'a> {
    access_mode: AccessMode,
    operations: &'a [Operation],
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
struct BatchResponse {
    #[serde(default)]
    results: Vec<Response>,
}

impl NeonAdapter {
    #[allow(dead_code)]
    pub fn new(endpoint: &str, bearer: Option<String>) -> Self {
        Self::new_with_options(endpoint, bearer, OpenOptions::default())
    }

    pub fn new_with_options(endpoint: &str, bearer: Option<String>, options: OpenOptions) -> Self {
        Self {
            endpoint: endpoint.trim_end_matches('/').to_owned(),
            bearer,
            client: reqwest::blocking::Client::new(),
            access_mode: options.access_mode,
        }
    }

    fn request(&self, sql: &str, params: &[Parameter]) -> Result<Response, DactylError> {
        let mut request = self.client.post(format!("{}/query", self.endpoint));
        if let Some(token) = &self.bearer {
            request = request.bearer_auth(token);
        }
        let response = request
            .json(&Request {
                sql,
                params: Some(params),
                access_mode: self.access_mode,
            })
            .send()
            .map_err(|error| {
                DactylError::adapter(AdapterErrorKind::Transport, format!("neon send: {error}"))
            })?;
        decode_response(response, "neon")
    }

    fn request_batch(&self, operations: &[Operation]) -> Result<BatchResponse, DactylError> {
        let mut request = self.client.post(format!("{}/batch", self.endpoint));
        if let Some(token) = &self.bearer {
            request = request.bearer_auth(token);
        }
        let response = request
            .json(&BatchRequest {
                access_mode: self.access_mode,
                operations,
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
        return Err(DactylError::adapter(
            AdapterErrorKind::Query,
            format!(
                "{operation} status {status}: {}",
                String::from_utf8_lossy(&body)
            ),
        ));
    }
    serde_json::from_slice(&body).map_err(|error| {
        DactylError::adapter(
            AdapterErrorKind::Protocol,
            format!("{operation} decode: {error}"),
        )
    })
}

impl Adapter for NeonAdapter {
    fn read(&self, sql: &str, params: &[Parameter]) -> Result<Rows, DactylError> {
        rows_from_response(self.request(sql, params)?)
    }

    fn write(&self, sql: &str, params: &[Parameter]) -> Result<WriteResult, DactylError> {
        let response = self.request(sql, params)?;
        Ok(WriteResult {
            affected_rows: response.affected_rows.unwrap_or(response.rows.len() as u64),
            generated_keys: response.generated_keys,
        })
    }

    fn atomic(&self, operations: &[Operation]) -> Result<AtomicResult, DactylError> {
        let response = self.request_batch(operations)?;
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
