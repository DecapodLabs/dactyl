//! Minimal SQL-over-HTTP transport for Neon.

use serde::{Deserialize, Serialize};

use crate::adapter::Adapter;
use crate::error::{AdapterErrorKind, DactylError};
use crate::rows::{Parameter, Row, Rows};

/// A short-lived Neon adapter. The endpoint owns SQL execution and business
/// logic; Dactyl only sends the request and normalizes the response.
pub struct NeonAdapter {
    endpoint: String,
    bearer: Option<String>,
    client: reqwest::blocking::Client,
}

#[derive(Debug, Serialize)]
struct Request<'a> {
    sql: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    params: Option<&'a [Parameter]>,
}

#[derive(Debug, Deserialize)]
struct Response {
    #[serde(default)]
    columns: Vec<String>,
    #[serde(default)]
    rows: Vec<serde_json::Value>,
    #[serde(default)]
    affected_rows: Option<u64>,
}

impl NeonAdapter {
    pub fn new(endpoint: &str, bearer: Option<String>) -> Self {
        Self {
            endpoint: endpoint.trim_end_matches('/').to_owned(),
            bearer,
            client: reqwest::blocking::Client::new(),
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
            })
            .send()
            .map_err(|error| {
                DactylError::adapter(AdapterErrorKind::Transport, format!("neon send: {error}"))
            })?;
        let status = response.status();
        let body = response.bytes().map_err(|error| {
            DactylError::adapter(
                AdapterErrorKind::Transport,
                format!("neon response: {error}"),
            )
        })?;
        if !status.is_success() {
            return Err(DactylError::adapter(
                AdapterErrorKind::Query,
                format!("neon status {status}: {}", String::from_utf8_lossy(&body)),
            ));
        }
        serde_json::from_slice(&body).map_err(|error| {
            DactylError::adapter(AdapterErrorKind::Protocol, format!("neon decode: {error}"))
        })
    }
}

impl Adapter for NeonAdapter {
    fn read(&self, sql: &str, params: &[Parameter]) -> Result<Rows, DactylError> {
        rows_from_response(self.request(sql, params)?)
    }

    fn write(&self, sql: &str, params: &[Parameter]) -> Result<u64, DactylError> {
        let response = self.request(sql, params)?;
        Ok(response.affected_rows.unwrap_or(response.rows.len() as u64))
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
