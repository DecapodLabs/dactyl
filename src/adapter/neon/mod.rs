//! Neon HTTP adapter.
//!
//! Thin client targeting Propodus. The adapter speaks JSON over HTTP; the
//! request shape is the contract for the conformance mock server:
//!
//! ```text
//! POST {endpoint}/read    { "sql": "...", "params": [...] }
//! POST {endpoint}/write   { "sql": "...", "params": [...] }
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

use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::adapter::Adapter;
use crate::error::DactylError;
use crate::rows::{Row, Rows};

/// Opaque handle to the Neon adapter.
#[derive(Clone)]
pub struct NeonAdapter {
    inner: Arc<Inner>,
}

struct Inner {
    endpoint: String,
    bearer: Option<String>,
    client: reqwest::blocking::Client,
}

#[derive(Debug, Serialize)]
struct QueryRequest<'a> {
    sql: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    params: Option<&'a serde_json::Value>,
    optimize: bool,
    write: bool,
}

#[derive(Debug, Deserialize, Serialize)]
struct QueryResponse {
    #[serde(default)]
    columns: Vec<String>,
    #[serde(default)]
    rows: Vec<serde_json::Value>,
}

impl NeonAdapter {
    /// Construct a Neon adapter pointed at the given Propodus endpoint.
    ///
    /// `bearer` is an opaque token forwarded in the `Authorization` header.
    /// `transport` is currently unused — Propodus decides HTTP vs HTTPS via
    /// the endpoint URL — but is accepted so the config surface is stable.
    pub fn new(endpoint: &str, bearer: Option<String>, _transport: Option<String>) -> Self {
        let client = reqwest::blocking::Client::builder()
            .build()
            .expect("reqwest blocking client");
        Self {
            inner: Arc::new(Inner {
                endpoint: endpoint.trim_end_matches('/').to_string(),
                bearer,
                client,
            }),
        }
    }
}

impl Adapter for NeonAdapter {
    fn name(&self) -> &'static str {
        "neon"
    }

    fn execute(
        &self,
        query: &str,
        params: Option<&serde_json::Value>,
        optimize: bool,
        write: bool,
    ) -> Result<Rows, DactylError> {
        let path = if write { "/write" } else { "/read" };
        let url = format!("{}{}", self.inner.endpoint, path);
        let req = QueryRequest {
            sql: query,
            params,
            optimize,
            write,
        };
        let mut rb = self.inner.client.post(&url).json(&req);
        if let Some(b) = &self.inner.bearer {
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
        let mut out = Vec::with_capacity(body.rows.len());
        for r in body.rows {
            let obj = r
                .as_object()
                .ok_or_else(|| DactylError::Adapter("neon row is not a JSON object".into()))?;
            if body.columns.is_empty() {
                // derive columns from first row's keys, preserve insertion order
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
}
