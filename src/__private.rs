//! Public-but-internal surface used by `dactyl_macros::query!`.

use crate::query::{Construct, QueryAnalyzer, Rewrite};

/// Result of running the analyzer at runtime (or at compile time, depending
/// on the call site).
pub struct RuntimeHit {
    /// SQL the analyzer says to execute (after any rewrite).
    pub sql: String,
    /// Constructs the analyzer found.
    pub constructs: Vec<Construct>,
}

/// Run the analyzer on `query` and return the rewritten SQL plus the
/// construct list. Cheap; the macro emits this at compile time so the
/// runtime path remains allocation-light.
pub fn analyze(query: &str) -> RuntimeHit {
    let analyzed = QueryAnalyzer::new().analyze(query);
    let sql = match &analyzed.rewrite {
        Rewrite::Identity => query.to_string(),
        Rewrite::Replaced(s) => s.clone(),
    };
    RuntimeHit {
        sql,
        constructs: analyzed.constructs,
    }
}
