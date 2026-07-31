//! Dactyl — the governed datastore boundary for Decapod.
//!
//! See the crate-level README for the contract. Public surface, intentionally:
//!
//! - [`init`]
//! - [`active_datastore`]
//! - [`read`]
//! - [`write`]
//! - [`query!`] (proc macro re-exported from `dactyl_macros`)
//!
//! Everything else lives behind `pub(crate)`, `pub mod`, or `pub` in private
//! modules.

pub mod adapter;
pub mod config;
pub mod error;
pub mod query;
mod rows;

#[doc(hidden)]
pub mod __private;

use std::sync::OnceLock;

pub use dactyl_macros::query;

pub use crate::config::DactylConfig;
pub use crate::error::DactylError;
pub use crate::rows::{Row, Rows};

/// Internal handle to the active datastore name. Populated by [`init`].
static ACTIVE: OnceLock<&'static str> = OnceLock::new();

/// Initialize dactyl with the given configuration.
///
/// Calling `init` more than once is permitted only if the same logical
/// datastore is re-registered; otherwise the second call returns
/// [`DactylError::Config`]. The most recent successful `init` wins for routing
/// in [`read`] / [`write`].
pub fn init(cfg: DactylConfig) -> Result<&'static str, DactylError> {
    let (datastore, result): (&'static str, Result<(), DactylError>) = match cfg {
        #[cfg(feature = "sqlite")]
        DactylConfig::Sqlite { path } => {
            let adapter = adapter::sqlite::SqliteAdapter::open(&path)
                .map_err(|e| DactylError::Config(format!("sqlite open: {e}")))?;
            adapter::register(std::sync::Arc::new(adapter));
            ("sqlite", Ok(()))
        }
        #[cfg(not(feature = "sqlite"))]
        DactylConfig::Sqlite { .. } => (
            "sqlite",
            Err(DactylError::Config(
                "sqlite adapter requested but `sqlite` feature is disabled".into(),
            )),
        ),
        #[cfg(feature = "neon")]
        DactylConfig::Neon {
            endpoint,
            bearer,
            transport,
        } => {
            let adapter = adapter::neon::NeonAdapter::new(&endpoint, bearer, transport);
            adapter::register(std::sync::Arc::new(adapter));
            ("neon", Ok(()))
        }
        #[cfg(not(feature = "neon"))]
        DactylConfig::Neon { .. } => (
            "neon",
            Err(DactylError::Config(
                "neon adapter requested but `neon` feature is disabled".into(),
            )),
        ),
    };
    result?;
    let _ = ACTIVE.set(datastore);
    Ok(datastore)
}

/// Return the logical datastore name the caller most recently passed to
/// [`init`], or `""` if dactyl has not been initialized.
pub fn active_datastore() -> &'static str {
    ACTIVE.get().copied().unwrap_or("")
}

/// Execute a read against the named datastore.
pub fn read(datastore: &str, query: &str, optimize: bool) -> Result<Rows, DactylError> {
    dispatch(datastore, query, optimize, false)
}

/// Execute a write against the named datastore.
pub fn write(datastore: &str, query: &str, optimize: bool) -> Result<Rows, DactylError> {
    dispatch(datastore, query, optimize, true)
}

fn dispatch(
    datastore: &str,
    query: &str,
    optimize: bool,
    write: bool,
) -> Result<Rows, DactylError> {
    // Look up the adapter by *datastore argument*. The inline `-- dactyl:`
    // directive only changes which dialect we treat as native for the
    // dialect-mismatch check below — routing still uses `datastore`.
    let adapter = adapter::lookup(datastore)
        .ok_or_else(|| DactylError::UnknownDatastore(datastore.to_string()))?;

    // Lex + dialect check.
    let analyzer = query::QueryAnalyzer::new();
    let analyzed = analyzer.analyze(query);

    let active_dialect = query::dialect_of(datastore)
        .ok_or_else(|| DactylError::UnknownDatastore(datastore.to_string()))?;

    let effective_dialect = analyzed
        .inline_override
        .and_then(query::dialect_of)
        .unwrap_or(active_dialect);

    if let Some(c) = query::first_unsupported(&analyzed.constructs, effective_dialect) {
        if !optimize {
            return Err(DactylError::DialectMismatch {
                datastore: datastore.to_string(),
                construct: c,
            });
        }
        // optimize = true: proceed; the rewriter currently emits identity.
    }

    let sql = analyzed.rewrite.apply(query);

    let params = serde_json::Value::Null;
    adapter.execute(&sql, Some(&params), optimize, write)
}
