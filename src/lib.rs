//! Dactyl — the governed datastore boundary for Decapod.
//!
//! The public API is intentionally tiny. There is no `init`. There is no
//! configuration step. The first call to [`read`] or [`write`] establishes
//! the connection.
//!
//! ```ignore
//! use dactyl::Rows;
//!
//! let rows: Rows = dactyl::read("select id, title from todos", true)?;
//! ```
//!
//! ## How dactyl picks the adapter
//!
//! On the first call, dactyl consults the environment for the target adapter:
//!
//! - `DATASTORE` — must be set to either `"sqlite"` or `"neon"`.
//! - `DATASTORE_ROUTE` — when `DATASTORE` is `"sqlite"`, this is the path to the SQLite file.
//!   When `DATASTORE` is `"neon"`, this is the Propodus endpoint URL.
//!
//! If the new `DATASTORE` variable is not set, dactyl falls back to the legacy environment variables
//! (`DACTYL_NEON_ENDPOINT`, `DACTYL_NEON_BEARER`, `DACTYL_SQLITE_PATH`, `DACTYL_SQLITE_ROOT`) for backwards compatibility.
//!
//! The connection is held in a `OnceLock` for the lifetime of the process.

pub mod adapter;
pub mod error;
pub mod query;
mod rows;

#[doc(hidden)]
pub mod __private;

pub use dactyl_db_macros::query;

pub use crate::error::DactylError;
pub use crate::rows::{Row, Rows};

use std::sync::{Arc, Mutex, OnceLock};

use crate::adapter::Adapter;

/// Lazy connections, keyed by the connection string. Populated by the first
/// `read` / `write` call for a given key. Tests may reset it.
static CONNECTIONS: OnceLock<Mutex<std::collections::HashMap<String, Arc<dyn Adapter>>>> =
    OnceLock::new();

fn connections() -> &'static Mutex<std::collections::HashMap<String, Arc<dyn Adapter>>> {
    CONNECTIONS.get_or_init(|| Mutex::new(std::collections::HashMap::new()))
}

/// Reset the cached connections. Test-only helper exposed to integration
/// tests; production code should never call this.
#[doc(hidden)]
pub fn __reset_for_tests() {
    let mut guard = connections()
        .lock()
        .expect("dactyl connection lock poisoned");
    guard.clear();
}

/// Execute a read against the dactyl connection.
///
/// The first call lazily establishes the connection (see module docs for the
/// selection rules). Subsequent calls reuse it.
pub fn read(query: &str, optimize: bool) -> Result<Rows, DactylError> {
    dispatch(query, optimize, false)
}

/// Execute a write against the dactyl connection.
pub fn write(query: &str, optimize: bool) -> Result<Rows, DactylError> {
    dispatch(query, optimize, true)
}

fn validate_env() -> Result<(), DactylError> {
    if let Ok(ds) = std::env::var("DATASTORE") {
        if ds != "sqlite" && ds != "neon" {
            return Err(DactylError::Adapter(
                "invalid DATASTORE value: must be 'sqlite' or 'neon'".to_string(),
            ));
        }
        if std::env::var("DATASTORE_ROUTE").is_err() {
            return Err(DactylError::Adapter(
                "DATASTORE_ROUTE is required when DATASTORE is set".into(),
            ));
        }
    } else {
        let has_legacy = std::env::var("DACTYL_NEON_ENDPOINT").is_ok()
            || std::env::var("DACTYL_SQLITE_PATH").is_ok();
        if !has_legacy {
            return Err(DactylError::Adapter(
                "no adapter configured: set DATASTORE and DATASTORE_ROUTE".into(),
            ));
        }
    }
    Ok(())
}

fn dispatch(query: &str, optimize: bool, write: bool) -> Result<Rows, DactylError> {
    validate_env()?;

    let analyzer = query::QueryAnalyzer::new();
    let analyzed = analyzer.analyze(query);

    // Dialect for the mismatch check. The inline `-- dactyl: <store>`
    // directive overrides the env-derived dialect so a single query can
    // target a different adapter for routing-only purposes.
    let inferred_dialect = infer_dialect(&analyzed);

    // Pick the adapter up-front so we can enforce the dialect check.
    let key = connection_key().ok_or_else(|| {
        DactylError::Adapter("no adapter configured: set DATASTORE and DATASTORE_ROUTE".into())
    })?;
    let adapter = connection(&key, query, &analyzed)?;
    if !optimize {
        if let Some(c) = query::first_unsupported(&analyzed.constructs, inferred_dialect) {
            return Err(DactylError::Unsupported { construct: c });
        }
    }

    let sql = analyzed.rewrite.apply(query);
    let params = serde_json::Value::Null;
    adapter.execute(&sql, Some(&params), optimize, write)
}

fn infer_dialect(analyzed: &query::Analyzed) -> query::Dialect {
    // The dialect we treat as "native" for the dialect-mismatch check.
    // The inline `-- dactyl: <store>` directive wins when present; otherwise
    // we fall back to whichever adapter is configured in the environment.
    if let Some(override_ds) = analyzed.inline_override {
        return match override_ds {
            "neon" => query::Dialect::Postgres,
            _ => query::Dialect::Sqlite,
        };
    }
    if let Ok(ds) = std::env::var("DATASTORE") {
        return match ds.as_str() {
            "neon" => query::Dialect::Postgres,
            _ => query::Dialect::Sqlite,
        };
    }
    if std::env::var("DACTYL_NEON_ENDPOINT").is_ok() {
        query::Dialect::Postgres
    } else {
        query::Dialect::Sqlite
    }
}

/// Establish (or return) the adapter for the given key.
fn connection(
    key: &str,
    query: &str,
    analyzed: &query::Analyzed,
) -> Result<Arc<dyn Adapter>, DactylError> {
    {
        let guard = connections()
            .lock()
            .expect("dactyl connection lock poisoned");
        if let Some(existing) = guard.get(key) {
            return Ok(existing.clone());
        }
    }
    let adapter = build_adapter(query, analyzed)?;
    let mut guard = connections()
        .lock()
        .expect("dactyl connection lock poisoned");
    if let Some(existing) = guard.get(key) {
        return Ok(existing.clone());
    }
    guard.insert(key.to_string(), adapter.clone());
    Ok(adapter)
}

/// Compute the cache key for the current env config. SQLite paths use the
/// resolved file path; neon uses the endpoint URL.
fn connection_key() -> Option<String> {
    if let Ok(ds) = std::env::var("DATASTORE") {
        if let Ok(route) = std::env::var("DATASTORE_ROUTE") {
            return Some(format!("{ds}:{route}"));
        }
    }
    if let Ok(endpoint) = std::env::var("DACTYL_NEON_ENDPOINT") {
        return Some(format!("neon:{endpoint}"));
    }
    if let Ok(path) = std::env::var("DACTYL_SQLITE_PATH") {
        return Some(format!("sqlite:{path}"));
    }
    None
}

#[cfg(feature = "sqlite")]
fn sqlite_adapter(path: &str) -> Result<Arc<dyn Adapter>, DactylError> {
    use crate::adapter::sqlite::SqliteAdapter;
    let adapter =
        SqliteAdapter::open(path).map_err(|e| DactylError::Adapter(format!("sqlite open: {e}")))?;
    Ok(Arc::new(adapter))
}

#[cfg(feature = "neon")]
fn neon_adapter() -> Result<Arc<dyn Adapter>, DactylError> {
    use crate::adapter::neon::NeonAdapter;
    let (endpoint, bearer) = resolve_neon_config()?;
    let adapter = NeonAdapter::new(&endpoint, bearer, None);
    Ok(Arc::new(adapter))
}

#[cfg(not(feature = "sqlite"))]
fn sqlite_adapter(_path: &str) -> Result<Arc<dyn Adapter>, DactylError> {
    Err(DactylError::Adapter(
        "sqlite adapter requested but `sqlite` feature is disabled".into(),
    ))
}

#[cfg(not(feature = "neon"))]
fn neon_adapter() -> Result<Arc<dyn Adapter>, DactylError> {
    Err(DactylError::Adapter(
        "neon adapter requested but `neon` feature is disabled".into(),
    ))
}

#[cfg(feature = "neon")]
fn resolve_neon_config() -> Result<(String, Option<String>), DactylError> {
    if let Ok(ds) = std::env::var("DATASTORE") {
        if ds == "neon" {
            let route = std::env::var("DATASTORE_ROUTE")
                .map_err(|_| DactylError::Adapter("DATASTORE_ROUTE not set".into()))?;
            let token = std::env::var("DATASTORE_TOKEN")
                .ok()
                .or_else(|| std::env::var("DACTYL_NEON_BEARER").ok());
            return Ok((route, token));
        }
    }
    let endpoint = std::env::var("DACTYL_NEON_ENDPOINT")
        .map_err(|_| DactylError::Adapter("DACTYL_NEON_ENDPOINT not set".into()))?;
    let bearer = std::env::var("DACTYL_NEON_BEARER").ok();
    Ok((endpoint, bearer))
}

fn resolve_sqlite_path(query: &str) -> Result<String, DactylError> {
    if let Ok(ds) = std::env::var("DATASTORE") {
        if ds == "sqlite" {
            let route = std::env::var("DATASTORE_ROUTE")
                .map_err(|_| DactylError::Adapter("DATASTORE_ROUTE not set".into()))?;
            return Ok(route);
        }
    }
    if let Ok(p) = std::env::var("DACTYL_SQLITE_PATH") {
        return Ok(p);
    }
    let default_root =
        std::env::var("DACTYL_SQLITE_ROOT").unwrap_or_else(|_| ".decapod/data".to_string());
    let store = infer_store(query).unwrap_or_else(|| "dactyl".to_string());
    Ok(format!("{default_root}/{store}.db"))
}

fn build_adapter(query: &str, analyzed: &query::Analyzed) -> Result<Arc<dyn Adapter>, DactylError> {
    let neon_env = if let Ok(ds) = std::env::var("DATASTORE") {
        ds == "neon"
    } else {
        std::env::var("DACTYL_NEON_ENDPOINT").is_ok()
    };
    let sqlite_only = !analyzed.constructs.is_empty()
        && analyzed
            .constructs
            .iter()
            .all(|c| c.dialect() == query::Dialect::Sqlite);
    let postgres_only = !analyzed.constructs.is_empty()
        && analyzed
            .constructs
            .iter()
            .all(|c| c.dialect() == query::Dialect::Postgres);

    if neon_env && !sqlite_only {
        neon_adapter()
    } else if !neon_env && postgres_only {
        // Caller is asking for postgres but hasn't configured the endpoint.
        // Fall back to sqlite (which will fail at the adapter); the caller
        // gets a clear error rather than a silent success.
        sqlite_adapter(&resolve_sqlite_path(query)?)
    } else {
        sqlite_adapter(&resolve_sqlite_path(query)?)
    }
}

/// Extract the first `from <name>` (or first table-shaped identifier) from
/// the query. Used to pick a default SQLite path when the caller hasn't
/// supplied one.
fn infer_store(query: &str) -> Option<String> {
    let lower = query.to_ascii_lowercase();
    let mut iter = lower.split_whitespace();
    while let Some(tok) = iter.next() {
        if tok == "from" || tok == "into" || tok == "update" || tok == "table" {
            if let Some(name) = iter.next() {
                return Some(name.trim_end_matches(';').to_string());
            }
        }
    }
    None
}
