//! Neon HTTP adapter.
//!
//! Thin SQL-over-HTTP client targeting Propodus. The adapter is constructed
//! per call from [`crate::build_adapter`] and lives for the duration of that
//! call. The request shape is the contract for the conformance mock server:
//!
//! ```text
//! POST {endpoint}/query  { "sql": "...", "params": [...] }
//! POST {endpoint}/batch  { "statements": [...] }
//! ```
//!
//! ```json
//! {
//!   "columns": ["id", "title", "status"],
//!   "rows": [
//!     {"id": 1, "title": "...", "status": "..."},
//!     ...
//!   ]
//! }
//! ```
//!
//! Propodus owns auth; dactyl only forwards the opaque `bearer` token.

use serde::{Deserialize, Serialize};

use crate::adapter::Adapter;
use crate::error::DactylError;
use crate::rows::{Parameter, Row, Rows};
use crate::Statement;

/// Opaque handle to the Neon adapter.
pub struct NeonAdapter {
    endpoint: String,
    bearer: Option<String>,
    client: reqwest::blocking::Client,
}

#[derive(Debug, Serialize)]
struct QueryRequest<'a> {
    sql: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    params: Option<&'a [Parameter]>,
}

#[derive(Debug, Deserialize, Serialize)]
struct QueryResponse {
    #[serde(default)]
    columns: Vec<String>,
    #[serde(default)]
    rows: Vec<serde_json::Value>,
}

#[derive(Debug, Serialize)]
struct BatchRequest<'a> {
    statements: &'a [Statement],
}

#[derive(Debug, Deserialize, Serialize)]
struct BatchResponse {
    #[serde(default)]
    results: Vec<QueryResponse>,
}

impl NeonAdapter {
    /// Construct a Neon adapter pointed at the given Propodus endpoint.
    ///
    /// `bearer` is an opaque token forwarded in the `Authorization` header.
    pub fn new(endpoint: &str, bearer: Option<String>) -> Self {
        let client = reqwest::blocking::Client::builder()
            .build()
            .expect("reqwest blocking client");
        Self {
            endpoint: endpoint.trim_end_matches('/').to_string(),
            bearer,
            client,
        }
    }
}

fn rows_from_response(body: QueryResponse) -> Result<Rows, DactylError> {
    let mut out = Vec::with_capacity(body.rows.len());
    for r in body.rows {
        let obj = r
            .as_object()
            .ok_or_else(|| DactylError::Adapter("neon row is not a JSON object".into()))?;
        if body.columns.is_empty() {
            let cols: Vec<String> = obj.keys().cloned().collect();
            let vals: Vec<serde_json::Value> = cols
                .iter()
                .map(|c| obj.get(c).cloned().unwrap_or(serde_json::Value::Null))
                .collect();
            out.push(Row {
                columns: cols,
                values: vals,
            });
        } else {
            let vals: Vec<serde_json::Value> = body
                .columns
                .iter()
                .map(|c| obj.get(c).cloned().unwrap_or(serde_json::Value::Null))
                .collect();
            out.push(Row {
                columns: body.columns.clone(),
                values: vals,
            });
        }
    }
    Ok(Rows(out))
}

impl Adapter for NeonAdapter {
    fn execute(&self, query: &str, params: &[Parameter]) -> Result<Rows, DactylError> {
        let url = format!("{}/query", self.inner_endpoint());
        let req = QueryRequest {
            sql: query,
            params: Some(params),
        };
        let mut rb = self.client.post(&url).json(&req);
        if let Some(b) = &self.bearer {
            rb = rb.bearer_auth(b);
        }
        let resp = rb
            .send()
            .map_err(|e| DactylError::Adapter(format!("neon send: {e}")))?;
        let status = resp.status();
        let body: QueryResponse = resp
            .json()
            .map_err(|e| DactylError::Adapter(format!("neon decode: {e}")))?;
        if !status.is_success() {
            return Err(DactylError::Adapter(format!(
                "neon status {status}: {}",
                serde_json::to_string(&body).unwrap_or_default()
            )));
        }
        rows_from_response(body)
    }

    fn execute_raw(&self, query: &str, params: &[Parameter]) -> Result<u64, DactylError> {
        let rows = self.execute(query, params)?;
        Ok(rows.len() as u64)
    }

    fn execute_batch(&self, statements: &[Statement]) -> Result<Vec<Rows>, DactylError> {
        let url = format!("{}/batch", self.inner_endpoint());
        let req = BatchRequest { statements };
        let mut rb = self.client.post(&url).json(&req);
        if let Some(b) = &self.bearer {
            rb = rb.bearer_auth(b);
        }
        let resp = rb
            .send()
            .map_err(|e| DactylError::Adapter(format!("neon batch send: {e}")))?;
        let status = resp.status();
        let body: BatchResponse = resp
            .json()
            .map_err(|e| DactylError::Adapter(format!("neon batch decode: {e}")))?;
        if !status.is_success() {
            return Err(DactylError::Adapter(format!(
                "neon batch status {status}: {}",
                serde_json::to_string(&body).unwrap_or_default()
            )));
        }
        let mut results = Vec::with_capacity(body.results.len());
        for res in body.results {
            results.push(rows_from_response(res)?);
        }
        Ok(results)
    }
}

impl NeonAdapter {
    fn inner_endpoint(&self) -> &str {
        &self.endpoint
    }
}
