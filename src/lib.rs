//! Dactyl — the governed datastore boundary for Decapod.
//!
//! Interchangeably read and write to local SQLite or cloud-hosted Vercel Neon
//! instances behind a single unified facade.

pub mod adapter;
pub mod error;
pub mod query;
mod rows;

#[doc(hidden)]
pub mod __private;

pub use dactyl_db_macros::query;

pub use crate::error::DactylError;
pub use crate::rows::{Parameter, Row, Rows};

use std::sync::{Arc, Mutex, OnceLock};

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

/// Lazy connections, keyed by the connection string. Populated by the first
/// datastore call for a given key.
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

/// Explicitly configure the active datastore and route.
///
/// Sets the `DATASTORE` and `DATASTORE_ROUTE` environment variables,
/// and optionally `DATASTORE_TOKEN`.
pub fn init(datastore: &str, route: &str, token: Option<&str>) {
    std::env::set_var("DATASTORE", datastore);
    std::env::set_var("DATASTORE_ROUTE", route);
    if let Some(t) = token {
        std::env::set_var("DATASTORE_TOKEN", t);
    } else {
        std::env::remove_var("DATASTORE_TOKEN");
    }
}

/// Reset/clear all cached connections.
pub fn reset() {
    __reset_for_tests();
}

/// Execute a parameterized read query.
pub fn read(query: &str, params: &[Parameter], optimize: bool) -> Result<Rows, DactylError> {
    dispatch(query, params, optimize, false)
}

/// Execute a parameterized write query.
pub fn write(query: &str, params: &[Parameter], optimize: bool) -> Result<Rows, DactylError> {
    dispatch(query, params, optimize, true)
}

/// Execute a raw schema/DDL/migration operation.
pub fn execute(query: &str, params: &[Parameter]) -> Result<u64, DactylError> {
    validate_env()?;
    let analyzer = query::QueryAnalyzer::new();
    let analyzed = analyzer.analyze(query);
    let key = connection_key().ok_or_else(|| {
        DactylError::Adapter("no adapter configured: set DATASTORE and DATASTORE_ROUTE".into())
    })?;
    let adapter = connection(&key, query, &analyzed)?;
    let sql = analyzed.rewrite.apply(query);
    adapter.execute_raw(&sql, params)
}

/// Execute an atomic batch of statements.
pub fn transaction(statements: &[Statement]) -> Result<Vec<Rows>, DactylError> {
    if statements.is_empty() {
        return Ok(Vec::new());
    }
    validate_env()?;
    let analyzer = query::QueryAnalyzer::new();
    // Analyze first query to decide which connection/dialect to use
    let analyzed = analyzer.analyze(&statements[0].sql);
    let key = connection_key().ok_or_else(|| {
        DactylError::Adapter("no adapter configured: set DATASTORE and DATASTORE_ROUTE".into())
    })?;
    let adapter = connection(&key, &statements[0].sql, &analyzed)?;

    // We apply rewrites to all statements in the batch
    let rewritten: Vec<Statement> = statements
        .iter()
        .map(|s| {
            let a = analyzer.analyze(&s.sql);
            Statement {
                sql: a.rewrite.apply(&s.sql),
                params: s.params.clone(),
            }
        })
        .collect();

    adapter.execute_batch(&rewritten)
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

fn dispatch(
    query: &str,
    params: &[Parameter],
    optimize: bool,
    write: bool,
) -> Result<Rows, DactylError> {
    validate_env()?;

    let analyzer = query::QueryAnalyzer::new();
    let analyzed = analyzer.analyze(query);

    let inferred_dialect = infer_dialect(&analyzed);

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
    adapter.execute(&sql, params, optimize, write)
}

fn infer_dialect(analyzed: &query::Analyzed) -> query::Dialect {
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

    if neon_env && !sqlite_only {
        neon_adapter()
    } else {
        sqlite_adapter(&resolve_sqlite_path(query)?)
    }
}

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
