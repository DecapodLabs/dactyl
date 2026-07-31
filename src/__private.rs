//! Public-but-internal surface used by `dactyl_macros::query!`.
//!
//! This module is `#[doc(hidden)]` from the public API's perspective but
//! reachable as `dactyl::__private` for the proc macro.

use crate::error::DactylError;
use crate::query::{first_unsupported, Construct, Dialect, QueryAnalyzer, Rewrite};

/// Runtime analysis result exposed to the macro.
pub struct RuntimeHit {
    /// SQL the macro should execute (after the analyzer's rewrite).
    pub sql: String,
    /// Construct list the analyzer found.
    pub constructs: Vec<Construct>,
}

/// Run the runtime analyzer. Mirrors what `lib::read` / `lib::write` do.
pub fn analyze_runtime(query: &str) -> RuntimeHit {
    let analyzer = QueryAnalyzer::new();
    let analyzed = analyzer.analyze(query);
    let sql = match &analyzed.rewrite {
        Rewrite::Identity => query.to_string(),
        Rewrite::Replaced(s) => s.clone(),
    };
    RuntimeHit {
        sql,
        constructs: analyzed.constructs,
    }
}

/// Whether every construct is portable for the given datastore.
pub fn runtime_portable(hit: &RuntimeHit, datastore: &str) -> bool {
    let Some(d) = dialect_for(datastore) else {
        return false;
    };
    first_unsupported(&hit.constructs, d).is_none()
}

fn dialect_for(datastore: &str) -> Option<Dialect> {
    match datastore {
        "sqlite" => Some(Dialect::Sqlite),
        "neon" | "postgres" | "postgresql" | "pg" => Some(Dialect::Postgres),
        _ => None,
    }
}

/// Construct a `DactylError::DialectMismatch` for the macro-generated runtime
/// check.
pub fn dialect_mismatch_err(datastore: &str, hit: &RuntimeHit) -> DactylError {
    let dialect = dialect_for(datastore).unwrap_or(Dialect::Sqlite);
    let construct = first_unsupported(&hit.constructs, dialect).unwrap_or(Construct::Strict);
    DactylError::DialectMismatch {
        datastore: datastore.to_string(),
        construct,
    }
}
