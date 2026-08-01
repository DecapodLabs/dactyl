//! Dactyl — the governed datastore boundary for Decapod.
//!
//! Interchangeably read and write to local SQLite or cloud-hosted Vercel Neon
//! instances behind a single unified facade.
//!
//! # Configuration
//!
//! The active datastore is selected by ambient environment variables — no
//! `init()` call and no per-call datastore argument:
//!
//! | Variable           | Required | Meaning                                                         |
//! |--------------------|----------|-----------------------------------------------------------------|
//! | `DATASTORE`        | yes      | `"sqlite"` or `"neon"`. Any other value is a typed error.        |
//! | `DATASTORE_ROUTE`  | yes      | SQLite file path (sqlite) or Neon/Propodus endpoint URL (neon).  |
//! | `DATASTORE_TOKEN`  | no       | Opaque bearer token forwarded to the Neon adapter. Ignored by sqlite. |
//!
//! Each call opens its own short-lived adapter execution and drops it on
//! return — there is no process-wide connection cache, so workspace and
//! session isolation is automatic and the public surface is `Send + Sync`
//! without any lock.
//!
//! # Public surface
//!
//! - [`query`]    — one entry point for any SQL (read or write).
//! - [`execute`]  — DDL / migration / affected-row operations.
//! - [`transaction`] — atomic batch of parameterized statements.
//! - [`query!`]   — compile-time SQL literal analyzer.
//!
//! No `init`, no `read`/`write` split, no `optimize` flag. The query analyzer
//! is still available via [`query::QueryAnalyzer`] for callers that want
//! compile-time dialect visibility; runtime dispatch always passes the SQL
//! straight to the active adapter.

pub mod adapter;
pub mod error;
pub mod query;
mod rows;

#[doc(hidden)]
pub mod __private;

pub use dactyl_db_macros::query;

pub use crate::error::DactylError;
pub use crate::rows::{Parameter, Row, Rows};

use crate::adapter::Adapter;

/// A parameterized SQL statement for batch execution.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq)]
pub struct Statement {
    pub sql: String,
    pub params: Vec<Parameter>,
}

impl Statement {
    /// Construct a new parameter-bound statement.
    pub fn new(sql: &str, params: Vec<Parameter>) -> Self {
        Self {
            sql: sql.to_string(),
            params,
        }
    }
}

/// Test-only helper retained for the conformance harness. With no process-wide
/// cache there is nothing to clear; the function is a no-op kept only so
/// existing tests do not have to be rewritten around its removal.
#[doc(hidden)]
pub fn reset() {}

/// Resolve the active datastore triple from ambient env vars.
///
/// Returns a typed error if `DATASTORE` is missing or unrecognized, or if
/// `DATASTORE_ROUTE` is missing.
fn resolve_env() -> Result<(&'static str, String, Option<String>), DactylError> {
    let kind = std::env::var("DATASTORE").map_err(|_| {
        DactylError::Adapter("DATASTORE is not set: set DATASTORE and DATASTORE_ROUTE".into())
    })?;
    let kind_static: &'static str = match kind.as_str() {
        "sqlite" => "sqlite",
        "neon" => "neon",
        other => {
            return Err(DactylError::Adapter(format!(
                "invalid DATASTORE value {other:?}: must be 'sqlite' or 'neon'"
            )))
        }
    };
    let route = std::env::var("DATASTORE_ROUTE").map_err(|_| {
        DactylError::Adapter("DATASTORE_ROUTE is not set: set DATASTORE and DATASTORE_ROUTE".into())
    })?;
    let token = std::env::var("DATASTORE_TOKEN").ok();
    Ok((kind_static, route, token))
}

/// Execute any SQL statement against the active datastore and return its
/// rows. Reads return the matched rows; writes return any returned rows
/// (Neon/`RETURNING`) or an empty [`Rows`] when the adapter surfaces no rows.
///
/// Parameters are bound by the adapter — never interpolated into the SQL.
/// The statement is executed against the datastore selected by the ambient
/// `DATASTORE` / `DATASTORE_ROUTE` / optional `DATASTORE_TOKEN` env vars.
pub fn query(sql: &str, params: &[Parameter]) -> Result<Rows, DactylError> {
    let (kind, route, token) = resolve_env()?;
    let adapter = build_adapter(kind, &route, token.as_deref())?;
    adapter.execute(sql, params)
}

/// Execute a DDL / migration / affected-row operation against the active
/// datastore and return the number of affected rows.
///
/// This is the caller-owned schema surface (dactyl #27): dactyl never
/// silently creates tables — callers own and version their schema through
/// explicit `execute` calls. Schemas opened against a caller-owned database
/// are not mutated beyond the statements the caller issues.
pub fn execute(sql: &str, params: &[Parameter]) -> Result<u64, DactylError> {
    let (kind, route, token) = resolve_env()?;
    let adapter = build_adapter(kind, &route, token.as_deref())?;
    adapter.execute_raw(sql, params)
}

/// Execute an atomic batch of parameterized statements.
///
/// On any per-statement error the whole unit rolls back and the function
/// returns the error; no partial state is committed. Equivalent semantics
/// are provided for SQLite (transaction) and Neon (`/batch` endpoint).
pub fn transaction(statements: &[Statement]) -> Result<Vec<Rows>, DactylError> {
    if statements.is_empty() {
        return Ok(Vec::new());
    }
    let (kind, route, token) = resolve_env()?;
    let adapter = build_adapter(kind, &route, token.as_deref())?;
    adapter.execute_batch(statements)
}

/// Construct a fresh, short-lived adapter for one call. No caching, no
/// shared mutable state — the returned adapter is dropped at the end of the
/// caller's call. Thread safety follows from this: nothing is shared between
/// calls, so there is no lock acquisition order to define.
fn build_adapter(
    kind: &str,
    route: &str,
    token: Option<&str>,
) -> Result<Box<dyn Adapter>, DactylError> {
    match kind {
        "sqlite" => {
            #[cfg(feature = "sqlite")]
            {
                let a = crate::adapter::sqlite::SqliteAdapter::open(route)
                    .map_err(|e| DactylError::Adapter(format!("sqlite open: {e}")))?;
                Ok(Box::new(a))
            }
            #[cfg(not(feature = "sqlite"))]
            {
                let _ = (route, token);
                Err(DactylError::Adapter(
                    "sqlite adapter requested but `sqlite` feature is disabled".into(),
                ))
            }
        }
        "neon" => {
            #[cfg(feature = "neon")]
            {
                let a = crate::adapter::neon::NeonAdapter::new(route, token.map(|s| s.to_string()));
                Ok(Box::new(a))
            }
            #[cfg(not(feature = "neon"))]
            {
                let _ = (route, token);
                Err(DactylError::Adapter(
                    "neon adapter requested but `neon` feature is disabled".into(),
                ))
            }
        }
        other => Err(DactylError::Adapter(format!(
            "unknown datastore {other:?}: must be 'sqlite' or 'neon'"
        ))),
    }
}
