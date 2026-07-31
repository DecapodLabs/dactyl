//! Adapter trait + dispatch table.
//!
//! Adapters are registered into a [`OnceLock`] keyed by datastore string in
//! [`crate::init`]. Reads and writes look up the adapter by name.

use std::collections::HashMap;
use std::sync::{Arc, OnceLock};

use crate::error::DactylError;
use crate::rows::Rows;

/// Trait every adapter implements.
///
/// The trait is intentionally narrow: it is the seam between dactyl's public
/// surface and the per-adapter implementation. New adapters implement this and
/// register themselves in [`register`].
pub trait Adapter: Send + Sync {
    /// Logical datastore name (e.g. `"sqlite"`, `"neon"`).
    fn name(&self) -> &'static str;

    /// Execute a read or write query against the backing datastore.
    ///
    /// `query` is the SQL string the analyzer has already (optionally)
    /// rewritten. `params` is the JSON-encodable list of bind values the
    /// caller passed (or `None` if the caller did not bind any). `optimize`
    /// is the caller's knob, propagated for adapter-side logging / telemetry.
    fn execute(
        &self,
        query: &str,
        params: Option<&serde_json::Value>,
        optimize: bool,
        write: bool,
    ) -> Result<Rows, DactylError>;
}

static REGISTRY: OnceLock<std::sync::Mutex<HashMap<&'static str, Arc<dyn Adapter>>>> =
    OnceLock::new();

fn registry() -> &'static std::sync::Mutex<HashMap<&'static str, Arc<dyn Adapter>>> {
    REGISTRY.get_or_init(|| std::sync::Mutex::new(HashMap::new()))
}

/// Register an adapter under its declared name. Overwrites any previous
/// registration for the same name.
pub fn register(adapter: Arc<dyn Adapter>) {
    let name = adapter.name();
    registry()
        .lock()
        .expect("dactyl adapter registry poisoned")
        .insert(name, adapter);
}

/// Look up an adapter by datastore name.
pub fn lookup(datastore: &str) -> Option<Arc<dyn Adapter>> {
    registry()
        .lock()
        .expect("dactyl adapter registry poisoned")
        .get(datastore)
        .map(Arc::clone)
}

/// Drop all registered adapters. Used by tests; not exposed publicly.
#[cfg(test)]
pub fn reset_for_tests() {
    registry()
        .lock()
        .expect("dactyl adapter registry poisoned")
        .clear();
}

#[cfg(feature = "sqlite")]
pub mod sqlite;

#[cfg(feature = "neon")]
pub mod neon;
